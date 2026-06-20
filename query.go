package lakedb

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func Query[T any](ctx context.Context, bucket *Bucket, filter T) ([]T, error) {
	boundaries := newBoundaries()
	filterValue := reflect.ValueOf(filter)
	if !filterValue.IsValid() {
		return nil, fmt.Errorf("invalid input filter type (expected table struct)")
	}
	for fieldMeta := range filterValue.Fields() {
		if !fieldMeta.IsExported() {
			continue
		}
		fieldName := ""
		tag := strings.SplitN(fieldMeta.Tag.Get("parquet"), ",", 1)
		if len(tag) < 1 || tag[0] == "" {
			fieldName = fieldMeta.Name
		} else {
			fieldName = tag[0]
		}
		switch field := filterValue.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Int:
			boundaries.Ints[fieldName] = IntBoundary{Max: field.filterMax, Min: field.filterMin}
		case Double:
			boundaries.Doubles[fieldName] = DoubleBoundary{Max: field.filterMax, Min: field.filterMin}
		}
	}

	rows := []T{}
	err := bucket.Lookup(ctx, getTableName(filterValue), boundaries, map[string]checkFilter{}, func(file io.ReaderAt, rowIdx int64) bool {
		reader := parquet.NewGenericReader[T](file)
		defer reader.Close()
		rowsBuffer := make([]T, 1)
		_, err := reader.Read(rowsBuffer)
		if err != nil {
			println(err.Error())
		}
		rows = append(rows, rowsBuffer...)
		return true
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}
