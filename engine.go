package lake

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/megakuul/lakedb/catalog"
	"github.com/parquet-go/parquet-go"
)

// query is the internal api between the engine and the querybuilder.
// it defines all query stages:
// 1. range filters (compares numeral or alphabetical ranges against the catalog / parquet statistics)
// 2. check filters (perform exact fine grained filtering on values that passed the range filter).
// 3. limit applies to stop the filtering process.
// 4. grouping (uses fine grained filters to group rows into one or more "windows" (by default just one global window))
// 5. aggregators (takes the grouped "windows" and applies aggregation to each column to collapse the grouped rows)
type query struct {
	ranges      map[string]catalog.Range
	checks      map[string]func(parquet.Value) bool
	limit       int // if set to -1 there is no limit
	grouping    map[string]func(parquet.Value) string
	aggregators map[string]func([]parquet.Value) parquet.Value
}

// lookup uses the provided ranges and checks to efficiently find all matching rows.
func (b *Bucket) lookup(ctx context.Context, schema *parquet.Schema, q *query) ([]parquet.Row, error) {
	b.catalogLock.RLock()
	defer b.catalogLock.RUnlock()
	table, ok := b.catalog.Tables[schema.Name()]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", schema.Name())
	}

	rowGroups := []parquet.RowGroup{}
	for _, shard := range filterShards(table.Shards, q.ranges) {
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
	rowGroup, err := parquet.MergeRowGroups(rowGroups, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to merge row groups: %v", err)
	}

	// TODO this could be a bitset instead to reduce size from chunk.NumValues() * 1 byte -> chunk.NumValues() * 1 bit
	// but linus, why is this not a map anymore?
	// -> It's a tragedy I know, even the cpu cache misses the map ^^
	rows := make([]bool, rowGroup.NumRows())

	groupColumns := groupHashmap{}

	for _, chunk := range rowGroup.ColumnChunks() {
		columnName := rowGroup.Schema().Columns()[chunk.Column()][0]
		chunkCheck := func(parquet.Value) bool { return true }
		if check, ok := q.checks[columnName]; ok {
			chunkCheck = check
		}

		assignToGroup := func(parquet.Value) {}
		derive, ok := q.grouping[columnName]
		if ok {
			assignToGroup = func(value parquet.Value) {
				id := derive(value)
				if groups, ok := groupColumns[chunk.Column()]; ok {
					create := true
					for _, existing := range groups[id] {
						if parquet.Equal(existing.match, value) {
							existing.values = append(existing.values, value)
							create = false
							break
						}
					}
					if create {
						groups[id] = append(groups[id], groupBucket{match: value, values: []parquet.Value{value}})
					}
				} else {
					groupColumns[chunk.Column()] = map[string][]groupBucket{
						id: {{match: value, values: []parquet.Value{value}}},
					}
				}
			}
		}

		matches := make([]bool, rowGroup.NumRows())
		if err := scanChunk(chunk, matches, assignToGroup, q.ranges[columnName], chunkCheck); err != nil {
			return nil, fmt.Errorf("failed scan chunk: %v", err)
		}
		// take the set from the first column as base.
		if chunk.Column() == 0 {
			rows = matches
			continue
		}
		// subsequent matches will just remove non-matching values from base (column filters are always AND joined).
		for row, ok := range rows {
			if ok && matches[row] {
				continue
			}
			rows[row] = false
		}
	}

	// enforce limit
	count := 0
	for row, ok := range rows {
		if !ok {
			continue
		}
		count++
		if count > q.limit {
			rows[row] = false
		}
	}

	if len(q.aggregators) > 0 {
		result := make([]parquet.Row, 0)
		rows := map[string][]groupBucket{}
		for column, groups := range groupColumns {
			for id, buckets := range groups {
				for _, bucket := range buckets {
					if aggregate, ok := q.aggregators[column]; ok {
						rows[id] = append(rows[id])
						aggregated := aggregate(bucket.values)
					}
				}
			}
		}
		return result, nil
	}

	reader := rowGroup.Rows()
	defer reader.Close()

	result := make([]parquet.Row, count)
	for row, ok := range rows {
		if !ok {
			continue
		}
		if err = reader.SeekToRow(int64(row)); err != nil {
			return nil, fmt.Errorf("failed to seek row: %v", err)
		}
		_, err := reader.ReadRows(result[row : row+1])
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
	}
	return result, nil
}

type groupHashmap map[int]map[string][]groupBucket

type groupBucket struct {
	match  parquet.Value
	values []parquet.Value
}

// scanChunk checks the boundary for each page and applies the filter to each row in matching pages.
// It marks all passing rows in the provided rows map as true.
func scanChunk(chunk parquet.ColumnChunk, rows []bool, assignToGroup func(parquet.Value), filterRange catalog.Range, check func(parquet.Value) bool) error {
	pages := chunk.Pages()
	defer pages.Close()

	columnIndex, err := chunk.ColumnIndex()
	if err != nil {
		return fmt.Errorf("failed to read column index: %v", err)
	}
	offsetIndex, err := chunk.OffsetIndex()
	if err != nil {
		return fmt.Errorf("failed to read column index: %v", err)
	}

	scannablePages := []int64{}
	for i := range columnIndex.NumPages() {
		switch filterMax := filterRange.Max.(type) {
		case int64:
			if columnIndex.MinValue(i).Kind() != parquet.Int64 || columnIndex.MinValue(i).Int64() > filterMax {
				continue
			}
		case float64:
			if columnIndex.MinValue(i).Kind() != parquet.Double || columnIndex.MinValue(i).Double() > filterMax {
				continue
			}
		case string:
			if columnIndex.MinValue(i).Kind() != parquet.ByteArray || string(columnIndex.MinValue(i).ByteArray()) > filterMax {
				continue
			}
		}
		switch filterMin := filterRange.Min.(type) {
		case int64:
			if columnIndex.MinValue(i).Kind() != parquet.Int64 || columnIndex.MaxValue(i).Int64() < filterMin {
				continue
			}
		case float64:
			if columnIndex.MinValue(i).Kind() != parquet.Double || columnIndex.MaxValue(i).Double() < filterMin {
				continue
			}
		case string:
			if columnIndex.MinValue(i).Kind() != parquet.ByteArray || string(columnIndex.MaxValue(i).ByteArray()) < filterMin {
				continue
			}
		}
		scannablePages = append(scannablePages, offsetIndex.FirstRowIndex(i))
	}

	for _, firstPageRow := range scannablePages {
		err := pages.SeekToRow(firstPageRow)
		if err != nil {
			return fmt.Errorf("failed to seek to page row: %v", err)
		}
		page, err := pages.ReadPage()
		if err != nil {
			return fmt.Errorf("failed to read page: %v", err)
		}

		values := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read rows: %v", err)
		}
		for valueIdx, value := range values[:n] {
			if !check(value) {
				continue
			}
			if int(firstPageRow)+valueIdx >= len(rows) {
				return fmt.Errorf("more rows then values in column chunk this is not allowed by lakedb!")
			}
			rows[firstPageRow+int64(valueIdx)] = true

			assignToGroup(value)
		}
	}
	return nil
}

// filterShards filters the shards based on the provided ranges (filter and shard range must overlap on every filter column to match).
func filterShards(shards []catalog.Shard, filter map[string]catalog.Range) []catalog.Shard {
	filteredShards := []catalog.Shard{}

Shards:
	for _, shard := range shards {
		for column, shardRange := range shard.Ranges {
			filterRange, ok := filter[column]
			if !ok {
				continue // unfiltered columns pass the filter
			}
			switch filterMax := filterRange.Max.(type) {
			case int64:
				if shardMin, ok := shardRange.Min.(int64); !ok || shardMin > filterMax {
					continue Shards
				}
			case float64:
				if shardMin, ok := shardRange.Min.(float64); !ok || shardMin > filterMax {
					continue Shards
				}
			case string:
				if shardMin, ok := shardRange.Min.(string); !ok || shardMin > filterMax {
					continue Shards
				}
			}
			switch filterMin := filterRange.Min.(type) {
			case int64:
				if shardMax, ok := shardRange.Max.(int64); !ok || shardMax < filterMin {
					continue Shards
				}
			case float64:
				if shardMax, ok := shardRange.Max.(float64); !ok || shardMax < filterMin {
					continue Shards
				}
			case string:
				if shardMax, ok := shardRange.Max.(string); !ok || shardMax < filterMin {
					continue Shards
				}
			}
		}
		filteredShards = append(filteredShards, shard)
	}
	return filteredShards
}
