package lakedb

import (
	"context"
	"fmt"
	"maps"
	"reflect"

	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

// QueryBuilder wraps the query structure with api exposed methods to construct it.
// Generics are not strictly required here but make the api more userfriendly for autocompletion.
type QueryBuilder[T Table] struct {
	query
}

func Query[T Table]() *QueryBuilder[T] {
	return &QueryBuilder[T]{query: query{
		ranges:     map[string]catalog.Range{},
		checks:     map[string]func(parquet.Value) bool{},
		grouping:   map[string][]func(parquet.Value) bool{},
		aggregator: map[string][]func([]parquet.Value) parquet.Value{},
	}}
}

func (b *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	b.limit = limit
	return b
}

func (b *QueryBuilder[T]) Where(filter T) *QueryBuilder[T] {
	ranges := map[string]catalog.Range{}
	checks := map[string]func(parquet.Value) bool{}
	filterValue := reflect.ValueOf(filter)
	if !filterValue.IsValid() {
		panic("invalid input filter type (expected table struct)")
	}
	for columnMeta := range filterValue.Fields() {
		if !columnMeta.IsExported() {
			continue
		}
		columnName := getColumnName(columnMeta)
		if filter, ok := filterValue.FieldByIndex(columnMeta.Index).Interface().(rangeFilter); ok {
			ranges[columnName] = catalog.Range{Max: filter.max(), Min: filter.min()}
		}
		if filter, ok := filterValue.FieldByIndex(columnMeta.Index).Interface().(genericFilter); ok {
			checks[columnName] = filter.filter
		}
	}
	maps.Copy(b.ranges, ranges)
	maps.Copy(b.checks, checks)
	return b
}

func (b *QueryBuilder[T]) Scan(ctx context.Context, bucket *Bucket, v []T) error {
	pseudo := *new(T)
	schema := parquet.NewSchema(pseudo.Name(), parquet.SchemaOf(pseudo))
	rows, err := bucket.lookup(ctx, schema, b.ranges, b.checks)
	if err != nil {
		return err
	}
	for _, row := range rows {
		var outputRow T
		if err = schema.Reconstruct(&outputRow, row); err != nil {
			return fmt.Errorf("failed to deserialize row: %v", err)
		}
		v = append(v, outputRow)
	}
	return nil
}

func (b *QueryBuilder[T]) Aggregate(ctx context.Context, bucket *Bucket, windows []any) error {
	groupings := map[string][]func(parquet.Value) bool{}
	aggregators := map[string][]func([]parquet.Value) parquet.Value{}
	for i, window := range windows {
		windowValue := reflect.ValueOf(window)
		if !windowValue.IsValid() {
			panic("invalid aggregator window type (expected struct)")
		}
		for columnMeta := range windowValue.Fields() {
			if !columnMeta.IsExported() {
				continue
			}
			columnName := getColumnName(columnMeta)

			// by default an aggregation window groups EVERYTHING (global aggregation)
			// window columns can optionally specify a generic filter to block out rows from this window.
			groupFilter := func(parquet.Value) bool { return true }
			if filter, ok := windowValue.FieldByIndex(columnMeta.Index).Interface().(genericFilter); ok {
				groupFilter = filter.filter
			}
			groupings[columnName] = append(groupings[columnName], groupFilter)

			// aggregators per window and per column are required so that the engine understands how to calculate the result.
			if aggregator, ok := windowValue.FieldByIndex(columnMeta.Index).Interface().(genericAggregator); ok {
				aggregators[columnName] = append(aggregators[columnName], aggregator.aggregate)
			} else {
				panic(fmt.Sprintf(
					"aggregation window '%d' does not specify an aggregation operation for column '%s' this is not allowed!", i, columnName,
				))
			}
		}
	}

	pseudo := *new(T)
	schema := parquet.NewSchema(pseudo.Name(), parquet.SchemaOf(pseudo))
	rows, err := bucket.lookup(ctx, schema, b.ranges, b.checks)
	if err != nil {
		return err
	}
	for i, row := range rows {
		if err = schema.Reconstruct(&windows[i], row); err != nil {
			return fmt.Errorf("failed to deserialize row: %v", err)
		}
	}
	return nil
}
