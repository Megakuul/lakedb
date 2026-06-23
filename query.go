package lakedb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

func Query[T Table](ctx context.Context, bucket *Bucket, filter T) ([]T, error) {
	ranges := map[string]catalog.Range{}
	checks := map[string]func(parquet.Value) bool{}
	filterValue := reflect.ValueOf(filter)
	if !filterValue.IsValid() {
		return nil, fmt.Errorf("invalid input filter type (expected table struct)")
	}
	for fieldMeta := range filterValue.Fields() {
		if !fieldMeta.IsExported() {
			continue
		}
		fieldName := ""
		tag := strings.SplitN(fieldMeta.Tag.Get("parquet"), ",", 2)
		if len(tag) < 1 || tag[0] == "" {
			fieldName = strings.ToLower(fieldMeta.Name)
		} else {
			fieldName = tag[0]
		}
		if filter, ok := filterValue.FieldByIndex(fieldMeta.Index).Interface().(rangeFilter); ok {
			ranges[fieldName] = catalog.Range{Max: filter.max(), Min: filter.min()}
		}
		if filter, ok := filterValue.FieldByIndex(fieldMeta.Index).Interface().(genericFilter); ok {
			checks[fieldName] = filter.filter
		}
	}

	schema := parquet.NewSchema(filter.Name(), parquet.SchemaOf(*new(T)))
	rows, err := bucket.lookup(ctx, schema, ranges, checks)
	if err != nil {
		return nil, err
	}
	output := make([]T, 0, len(rows))
	for _, row := range rows {
		var outputRow T
		if err = schema.Reconstruct(&outputRow, row); err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %v", err)
		}
		output = append(output, outputRow)
	}
	return output, nil
}
