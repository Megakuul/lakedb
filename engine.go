package lake

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"io"
	"math/big"

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
	grouping    map[string]func(parquet.Value) (uint64, parquet.Value)
	aggregators map[string]func([]parquet.Value) parquet.Value
}

func (b *Bucket) process(ctx context.Context, schema *parquet.Schema, q *query) ([]parquet.Row, error) {
	rowGroup, err := b.load(ctx, schema, q)
	if err != nil {
		return nil, err
	}
	rows, err := b.filter(rowGroup, q)
	if err != nil {
		return nil, err
	}

	if q.limit > 0 {
		var (
			limitedRows big.Int
			count       int
		)
		for row := range rows.BitLen() {
			if rows.Bit(row) == 1 {
				count++
				if count > q.limit {
					break
				}
				limitedRows.SetBit(&limitedRows, row, 1)
			}
		}
		rows = &limitedRows
	}

	if len(q.aggregators) < 1 {
		return b.extract(rowGroup, rows) // without aggregators just extract and return full rows.
	}

	streams := newHashmap[big.Int]()
	if len(q.grouping) < 1 {
		streams.set(0, nil, *rows)
	} else {
		streams, err = b.group(rowGroup, rows, q)
		if err != nil {
			return nil, err
		}
	}
	return b.aggregate(rowGroup, streams, q)
}

func (b *Bucket) load(ctx context.Context, schema *parquet.Schema, q *query) (parquet.RowGroup, error) {
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
	return rowGroup, nil
}

func (b *Bucket) filter(rowGroup parquet.RowGroup, q *query) (*big.Int, error) {
	filterColumns, err := createFilterColumns(rowGroup.Schema(), q)
	if err != nil {
		return nil, err
	}

	// rows is a bitset compressed '[]bool{}' that maps row positions to their "match" status.
	var rows *big.Int
	for i, filterColumn := range filterColumns {
		chunk := rowGroup.ColumnChunks()[filterColumn.index]
		var matches big.Int
		if err := scanChunk(chunk, &matches, rows, filterColumn.chunkRange, filterColumn.chunkCheck); err != nil {
			return nil, fmt.Errorf("failed scan chunk: %v", err)
		}
		// first column captures all matches
		if i == 0 {
			rows = &matches
			continue
		}
		// other columns just remove bits from rows
		for row := range rows.BitLen() {
			if rows.Bit(row) == 1 && matches.Bit(row) == 0 {
				rows.SetBit(rows, row, 0)
			}
		}
	}
	return rows, nil
}

func (b *Bucket) group(rowGroup parquet.RowGroup, rows *big.Int, q *query) (*hashmap[big.Int], error) {
	hashes := make([][]uint64, rows.BitLen())
	keys := make([][]parquet.Value, rows.BitLen())

	for columnIdx, column := range rowGroup.Schema().Columns() {
		columnName := column[0]
		group, ok := q.grouping[columnName]
		if !ok {
			continue
		}
		chunk := rowGroup.ColumnChunks()[columnIdx]

		pages := chunk.Pages()
		defer pages.Close()

		offsetIndex, err := chunk.OffsetIndex()
		if err != nil {
			return nil, fmt.Errorf("failed to read offset index: %v", err)
		}

		for pageIdx := range offsetIndex.NumPages() {
			firstPageRow := offsetIndex.FirstRowIndex(pageIdx)

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
			for valueIdx, value := range values[:n] {
				rowIdx := int(firstPageRow) + valueIdx
				if rows.Bit(rowIdx) == 0 {
					continue
				}

				hash, key := group(value)
				hashes[rowIdx] = append(hashes[rowIdx], hash)
				keys[rowIdx] = append(keys[rowIdx], key)
			}
		}
	}

	streams := newHashmap[big.Int]()
	count := 0
	for row, hash := range hashes {
		if rows.Bit(row) == 0 {
			continue
		}
		count++
		if count > b.maxGroupRows {
			return nil, fmt.Errorf("maximum grouping size exceeded!")
		}

		streamHash, streamKey := maphash.Hash{}, keys[row]
		streamHash.SetSeed(mapSeed)
		for _, hash := range hash {
			var buffer [8]byte
			binary.LittleEndian.PutUint64(buffer[:], hash)
			streamHash.Write(buffer[:])
		}
		rawStreamHash := streamHash.Sum64()
		stream, _ := streams.get(rawStreamHash, streamKey)
		stream.SetBit(&stream, row, 1)
		streams.set(rawStreamHash, streamKey, stream)
	}
	return streams, nil
}

func (b *Bucket) aggregate(rowGroup parquet.RowGroup, streams *hashmap[big.Int], q *query) ([]parquet.Row, error) {
	results := []parquet.Row{}
	for hash, keys := range streams.keys() {
		rows, _ := streams.get(hash, keys)
		result := make(parquet.Row, len(rowGroup.Schema().Columns()))
		for columnIdx, column := range rowGroup.Schema().Columns() {
			columnName := column[0]
			aggregate, ok := q.aggregators[columnName]
			if !ok {
				result[columnIdx] = parquet.NullValue()
				continue
			}
			chunk := rowGroup.ColumnChunks()[columnIdx]

			pages := chunk.Pages()
			defer pages.Close()

			offsetIndex, err := chunk.OffsetIndex()
			if err != nil {
				return nil, fmt.Errorf("failed to read offset index: %v", err)
			}

			rawValues := []parquet.Value{}
			for pageIdx := range offsetIndex.NumPages() {
				firstPageRow := offsetIndex.FirstRowIndex(pageIdx)

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
				for valueIdx, value := range values[:n] {
					if rows.Bit(int(firstPageRow)+valueIdx) == 0 {
						continue
					}
					rawValues = append(rawValues, value)
				}
			}
			result[columnIdx] = aggregate(rawValues)
		}

		results = append(results, result)
	}
	return results, nil
}

func (b *Bucket) extract(rowGroup parquet.RowGroup, rows *big.Int) ([]parquet.Row, error) {
	reader := rowGroup.Rows()
	defer reader.Close()

	result := make([]parquet.Row, 0)
	for row := range rows.BitLen() {
		if rows.Bit(row) == 0 {
			continue
		}
		if err := reader.SeekToRow(int64(row)); err != nil {
			return nil, fmt.Errorf("failed to seek row: %v", err)
		}
		buffer := make([]parquet.Row, 1)
		_, err := reader.ReadRows(buffer)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read rows: %v", err)
		}
		result = append(result, buffer...)
	}
	return result, nil
}

// scanChunk checks the boundary for each page and applies the filter to each row in matching pages.
// It marks passing values in the matches bitset and skips filters on rows already filtered out in the skip bitset.
func scanChunk(chunk parquet.ColumnChunk, matches, skip *big.Int, filterRange catalog.Range, check func(parquet.Value) bool) error {
	pages := chunk.Pages()
	defer pages.Close()

	columnIndex, err := chunk.ColumnIndex()
	if err != nil {
		return fmt.Errorf("failed to read column index: %v", err)
	}
	offsetIndex, err := chunk.OffsetIndex()
	if err != nil {
		return fmt.Errorf("failed to read offset index: %v", err)
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
			if columnIndex.MaxValue(i).Kind() != parquet.Int64 || columnIndex.MaxValue(i).Int64() < filterMin {
				continue
			}
		case float64:
			if columnIndex.MaxValue(i).Kind() != parquet.Double || columnIndex.MaxValue(i).Double() < filterMin {
				continue
			}
		case string:
			if columnIndex.MaxValue(i).Kind() != parquet.ByteArray || string(columnIndex.MaxValue(i).ByteArray()) < filterMin {
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
			if skip != nil && skip.Bit(int(firstPageRow)+valueIdx) == 0 {
				continue
			}
			if check != nil && !check(value) {
				continue
			}
			matches.SetBit(matches, int(firstPageRow)+valueIdx, 1)
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
