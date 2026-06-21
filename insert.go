package lakedb

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
)

type Ingestor[T any] struct {
	table       string
	buffer      *parquet.GenericBuffer[T]
	bucket      *Bucket
	rangeBuffer Ranges
}

func NewIngestor[T any](bucket *Bucket) *Ingestor[T] {
	return &Ingestor[T]{
		table: getTableName(reflect.ValueOf(*new(T))),
		buffer: parquet.NewGenericBuffer[T](parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("timestamp"),
			),
		)),
		bucket:      bucket,
		rangeBuffer: newRanges(),
	}
}

func (i *Ingestor[T]) Insert(ctx context.Context, row T) error {
	rowValue := reflect.ValueOf(row)
	if !rowValue.IsValid() {
		return fmt.Errorf("row type is invalid (expected non-nil struct)")
	}
	for fieldMeta := range rowValue.Fields() {
		if !fieldMeta.IsExported() {
			continue
		}
		fieldName := ""
		tag := strings.SplitN(fieldMeta.Tag.Get("parquet"), ",", 2)
		if len(tag) < 2 || tag[0] == "" {
			fieldName = fieldMeta.Name
		} else {
			fieldName = tag[0]
		}
		switch field := rowValue.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Int:
			boundary := i.rangeBuffer.Ints[fieldName]
			if boundary.Max == nil || *boundary.Max < field.Data {
				boundary.Max = &field.Data
			}
			if boundary.Min == nil || *boundary.Min > field.Data {
				boundary.Min = &field.Data
			}
			i.rangeBuffer.Ints[fieldName] = boundary
		case Double:
			boundary := i.rangeBuffer.Doubles[fieldName]
			if boundary.Max == nil || *boundary.Max < field.Data {
				boundary.Max = &field.Data
			}
			if boundary.Min == nil || *boundary.Min > field.Data {
				boundary.Min = &field.Data
			}
			i.rangeBuffer.Doubles[fieldName] = boundary
		}
	}
	_, err := i.buffer.Write([]T{row})
	return err
}

func (i *Ingestor[T]) Close(ctx context.Context) error {
	sort.Sort(i.buffer)

	output := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[T](output)
	_, err := parquet.CopyRows(writer, i.buffer.Rows())
	if err != nil {
		return fmt.Errorf("failed to flush parquet buffer: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %v", err)
	}
	return i.bucket.Write(ctx, i.table, output.Bytes(), i.rangeBuffer)
}
