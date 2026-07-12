// Package catalog contains the structure of the lakedb index catalog format.
// The catalog serves as metadata to avoid hive partitioning which is not atomic and expensive due to ListBucket requests.
// Abstracted into its own package to avoid top level bloat (rarely used by end users).
package catalog

import "time"

type Catalog struct {
	Key     string           `json:"-"`
	ETag    *string          `json:"-"`
	Expires time.Time        `json:"-"`
	Version string           `json:"version"` // version is always v1 for now
	Tables  map[string]Table `json:"tables"`
}

type Table struct {
	Shards []Shard `json:"shards"`
}

type Shard struct {
	Size   int    `json:"size"`
	Target string `json:"target"`
	// range statistics defined per column.
	Ranges map[string]Range `json:"ranges"`
}

type ColumnKind int

const (
	ColumnString = iota
	ColumnInt
	ColumnFloat
)

type Range struct {
	Kind ColumnKind `json:"kind"`

	MaxEnabled bool `json:"max_enabled"`
	MinEnabled bool `json:"min_enabled"`

	MaxFloat float64 `json:"max_float,omitempty"`
	MinFloat float64 `json:"min_float,omitempty"`

	MaxInt int64 `json:"max_int,omitempty"`
	MinInt int64 `json:"min_int,omitempty"`

	MaxString string `json:"max_string,omitempty"`
	MinString string `json:"min_string,omitempty"`
}
