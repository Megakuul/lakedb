package lakedb

import (
	"context"
	"reflect"
	"strings"
	"time"
)

func Query(ctx context.Context, bucket *Bucket, filter any) error {
	err := bucket.Lookup(ctx, "test", Boundaries{
		Ints: map[string]IntBoundary{
			"timestamp": {Max: time.Now().Add(time.Hour).Unix(), Min: time.Now().Add(-time.Hour).Unix()},
		},
	})
	if err != nil {
		return err
	}
	return nil
	floatFields := map[string]Double{}
	filterVal := reflect.ValueOf(filter)
	for fieldMeta := range filterVal.Fields() {
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
		switch fieldFilter := filterVal.FieldByIndex(fieldMeta.Index).Interface().(type) {
		case Double:
			floatFields[fieldName] = fieldFilter
		}
	}

	return nil
}
