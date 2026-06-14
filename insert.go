package lakedb

import (
	"bytes"
	"context"
	"time"

	"github.com/parquet-go/parquet-go"
)

func Insert[T any](ctx context.Context, bucket *Bucket, data T) error {
	buffer := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[T](buffer)
	_, err := writer.Write([]T{data})
	if err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return bucket.Write(ctx, "test", buffer.Bytes(), Boundaries{
		Ints: map[string]IntBoundary{
			"timestamp": {Max: time.Now().Unix(), Min: time.Now().Unix()},
		},
	})
}
