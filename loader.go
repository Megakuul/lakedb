package lakedb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

// checkNumeralBoundary checks if the originalMin - originalMax range is INSIDE the filter range.
// Filters are optional, if one side is omitted everything on this side matches
// e.g. max == nil means original range must be between min - ∞.
func checkNumeralBoundary[T int64 | float64](originalMin, originalMax T, filterMin, filterMax *T) bool {
	if filterMin != nil && *filterMin > originalMax {
		return false
	}
	if filterMax != nil && *filterMax < originalMin {
		return false
	}
	return true
}

func (b *Bucket) lookup(ctx context.Context, tableName string, bounds Boundaries, filters map[string]checkFilter) ([]parquet.Row, error) {
	b.catalogLock.RLock()
	defer b.catalogLock.RUnlock()
	table, ok := b.catalog.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	rowGroups := []parquet.RowGroup{}
	for _, shard := range filterShards(table.Shards, bounds) {
		result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &b.name,
			Key:    &shard.Target,
		})
		if err != nil {
			return nil, err
		}
		defer result.Body.Close()
		buffer, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, err
		}
		file, err := parquet.OpenFile(bytes.NewReader(buffer), int64(shard.Size))
		if err != nil {
			return nil, fmt.Errorf("cannot open shard file '%s': %v", shard.Target, err)
		}
		rowGroups = append(rowGroups, file.RowGroups()...)
	}
	rowGroup, err := parquet.MergeRowGroups(rowGroups)
	if err != nil {
		return nil, fmt.Errorf("failed to merge row groups: %v", err)
	}

	rows := map[int64]struct{}{}

	for _, chunk := range rowGroup.ColumnChunks() {
		matches, err := scanRows(chunk, nil)
		if err != nil {
			return nil, fmt.Errorf("failed scan rows: %v", err)
		}
		if chunk.Column() == 0 {
			maps.Copy(rows, matches)
			continue
		}
		// convert rows to a subset of matches (remove filtered out rows).
		for row := range rows {
			if _, ok := matches[row]; ok {
				continue
			}
			delete(rows, row)
		}
	}

	reader := rowGroup.Rows()
	defer reader.Close()

	output := make([]parquet.Row, len(rows))
	for i, row := range slices.Sorted(maps.Keys(rows)) {
		if err = reader.SeekToRow(row); err != nil {
			return nil, fmt.Errorf("failed to seek row: %v", err)
		}
		_, err := reader.ReadRows(output[i : i+1])
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
	}
	return output, nil
}

type filter interface {
	AboveMax(parquet.Value) bool
	BelowMin(parquet.Value) bool
	Filter(parquet.Value) bool
}

// scanRows checks the boundary for each page and applies the filter to each rows in matching pages.
// Returns a map containing the global row index for each matching row.
func scanRows(chunk parquet.ColumnChunk, filter filter) (map[int64]struct{}, error) {
	pages := chunk.Pages()
	defer pages.Close()

	approved := map[int64]struct{}{}

	columnIndex, err := chunk.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to read column index: %v", err)
	}
	offsetIndex, err := chunk.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to read column index: %v", err)
	}

	scannablePages := []int64{}
	for i := range columnIndex.NumPages() {
		if filter.AboveMax(columnIndex.MinValue(i)) {
			continue
		}
		if filter.BelowMin(columnIndex.MaxValue(i)) {
			continue
		}
		scannablePages = append(scannablePages, offsetIndex.FirstRowIndex(i))
	}

	for _, firstPageRow := range scannablePages {
		err := pages.SeekToRow(firstPageRow)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to page row: %v", err)
		}
		page, err := pages.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("failed to read page: %v", err)
		}
		values := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
		println("len of values per page")
		println(n)
		for valueIdx, value := range values[:n] {
			if !filter.Filter(value) {
				continue
			}
			approved[firstPageRow+int64(valueIdx)] = struct{}{}
		}
	}
	return approved, nil
}

// filterShards filters the shards based on the provided bounds.
func filterShards(shards []Shard, bounds Boundaries) []Shard {
	filteredShards := []Shard{}
	for _, shard := range shards {
		for name, field := range shard.Boundaries.Ints {
			fieldFilter, ok := bounds.Ints[name]
			if ok && field.Min != nil && field.Max != nil {
				if !checkNumeralBoundary(*field.Min, *field.Max, fieldFilter.Min, fieldFilter.Max) {
					break
				}
			}
		}
		for name, field := range shard.Boundaries.Doubles {
			fieldFilter, ok := bounds.Doubles[name]
			if ok && field.Min != nil && field.Max != nil {
				if !checkNumeralBoundary(*field.Min, *field.Max, fieldFilter.Min, fieldFilter.Max) {
					break
				}
			}
		}
		filteredShards = append(filteredShards, shard)
	}
	return filteredShards
}
