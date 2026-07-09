package lake

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/megakuul/lakedb/internal/catalog"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// Ingestor provides a processor for one batch of input data.
type Ingestor[T any] struct {
	table  string
	buffer *bytes.Buffer
	writer *parquet.SortingWriter[T]
	bucket *Bucket
}

func NewIngestor[T any](bucket *Bucket) *Ingestor[T] {
	tableName, tableSorting := getMetadata(reflect.TypeFor[T]())
	buffer := bytes.NewBuffer(nil)
	return &Ingestor[T]{
		table:  tableName,
		buffer: buffer,
		writer: parquet.NewSortingWriter[T](buffer, 100_000, parquet.SortingWriterConfig(
			parquet.SortingColumns(tableSorting...),
		)),
		bucket: bucket,
	}
}

// insert writes the provided parquet row to the processor. This does NOT write anything to disk.
func (i *Ingestor[T]) Insert(rows ...T) error {
	_, err := i.writer.Write(rows)
	return err
}

// Close writes the ingested rows into the underlying storage.
func (i *Ingestor[T]) Close(ctx context.Context) error {
	if err := i.writer.Close(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %v", err)
	}
	schema := parquet.NewSchema(i.table, parquet.SchemaOf(*new(T)))
	ranges, err := extractRanges(schema, i.writer.File().Metadata().RowGroups)
	if err != nil {
		return fmt.Errorf("extracting ranges: %v", err)
	}
	return i.bucket.write(ctx, i.table, i.buffer.Bytes(), ranges)
}

// extractRanges reads the metadata from the provided row groups to calculate the catalog ranges per row.
func extractRanges(schema *parquet.Schema, rowGroups thrift.Slice[format.RowGroup]) (map[string]catalog.Range, error) {
	ranges := map[string]catalog.Range{}
	for _, rowGroup := range rowGroups {
		for column, chunk := range rowGroup.Columns {
			columnName := schema.Columns()[column][0]
			columnRange := ranges[columnName]

			leaf, _ := schema.Lookup(chunk.MetaData.PathInSchema...)
			kind := leaf.Node.Type().Kind()

			stats := chunk.MetaData.Statistics

			if len(stats.MinValue) < 1 || len(stats.MaxValue) < 1 {
				return nil, fmt.Errorf("invalid row group: min / max value statistics not set")
			}
			max := kind.Value(stats.MaxValue)
			switch max.Kind() {
			case parquet.Int64:
				if currentMax, ok := columnRange.Max.(int64); !ok || currentMax < max.Int64() {
					columnRange.Max = max.Int64()
				}
			case parquet.Double:
				if currentMax, ok := columnRange.Max.(float64); !ok || currentMax < max.Double() {
					columnRange.Max = max.Double()
				}
			case parquet.ByteArray:
				if currentMax, ok := columnRange.Max.(string); !ok || currentMax < string(max.ByteArray()) {
					columnRange.Max = string(max.ByteArray())
				}
			}
			min := kind.Value(stats.MinValue)
			switch min.Kind() {
			case parquet.Int64:
				if currentMin, ok := columnRange.Min.(int64); !ok || currentMin > min.Int64() {
					columnRange.Min = min.Int64()
				}
			case parquet.Double:
				if currentMin, ok := columnRange.Min.(float64); !ok || currentMin > min.Double() {
					columnRange.Min = min.Double()
				}
			case parquet.ByteArray:
				if currentMin, ok := columnRange.Min.(string); !ok || currentMin > string(min.ByteArray()) {
					columnRange.Min = string(min.ByteArray())
				}
			}
			ranges[columnName] = columnRange
		}
	}
	return ranges, nil
}
