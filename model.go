package lakedb

import (
	"github.com/parquet-go/parquet-go"
)

// Table is the interface that must be implemented by all parquet table structs.
type Table interface {
	// Name defines the table name in parquet. It is a hard contract to the data.
	Name() string
	// Sorting defines the table column sorting.
	Sorting() parquet.SortingOption
}
