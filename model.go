package lakedb

import (
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Table is the interface that must be implemented by all parquet table structs.
type Table interface {
	// Name defines the table name in parquet. It is a hard contract to the data.
	Name() string
	// Sorting defines the table column sorting.
	Sorting() parquet.SortingOption
}

// getColumnName extracts the parquet column name from a struct field.
// It is compatible to the parquet library struct tagging system.
func getColumnName(field reflect.StructField) string {
	tag := strings.SplitN(field.Tag.Get("parquet"), ",", 2)
	if len(tag) < 1 || tag[0] == "" {
		return strings.ToLower(field.Name)
	}
	return tag[0]
}
