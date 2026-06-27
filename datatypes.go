package lakedb

import (
	"strings"

	"github.com/parquet-go/parquet-go"
)

type genericAggregator interface {
	aggregate([]parquet.Value) parquet.Value
}

type genericFilter interface {
	filter(parquet.Value) bool
}

type rangeFilter interface {
	max() any
	min() any
}

type String struct {
	stringFilter
	Data string `parquet:"data"`
}

func NewString(value string) String {
	return String{Data: value}
}

type stringFilter struct {
	filterOps []func(string) bool
}

func (d stringFilter) filter(v parquet.Value) bool {
	if v.Kind() != parquet.ByteArray {
		return true
	}
	for _, op := range d.filterOps {
		if !op(string(v.ByteArray())) {
			return false
		}
	}
	return true
}

func NewStringFilter() *stringFilter {
	return new(stringFilter)
}

func (f *stringFilter) Eq(operand string) *stringFilter {
	f.filterOps = append(f.filterOps, func(v string) bool {
		return v == operand
	})
	return f
}

func (f *stringFilter) Ne(operand string) *stringFilter {
	f.filterOps = append(f.filterOps, func(v string) bool {
		return v != operand
	})
	return f
}

func (f *stringFilter) Contains(operand string) *stringFilter {
	f.filterOps = append(f.filterOps, func(v string) bool {
		return strings.Contains(v, operand)
	})
	return f
}

func (f *stringFilter) End() String {
	return String{*f, ""}
}
