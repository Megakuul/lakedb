package lake

import (
	"hash/maphash"

	"github.com/parquet-go/parquet-go"
)

var mapSeed = maphash.MakeSeed()

type Grouper func(parquet.Value) (uint64, parquet.Value)

func Exact(value parquet.Value) (uint64, parquet.Value) {
	return maphash.Bytes(mapSeed, value.Bytes()), value
}
