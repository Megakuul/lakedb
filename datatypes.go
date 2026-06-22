package lakedb

import (
	"strings"

	"github.com/parquet-go/parquet-go"
)

type genericFilter interface {
	filter(parquet.Value) bool
}

type rangeFilter interface {
	max() any
	min() any
}

type Int struct {
	Data   int64 `parquet:"data"`
	filter intFilter
}

func NewInt(value int64) Int {
	return Int{Data: value}
}

type intFilter struct {
	maxValue  *int64
	minValue  *int64
	filterOps []func(int64) bool
}

func (d *intFilter) max() any {
	return d.maxValue
}

func (d *intFilter) min() any {
	return d.minValue
}

func (d *intFilter) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Int64 {
		return true
	}
	for _, op := range d.filterOps {
		if !op(v.Int64()) {
			return false
		}
	}
	return true
}

func NewIntFilter() *intFilter {
	return new(intFilter)
}

func (d *intFilter) Gte(operand int64) *intFilter {
	d.minValue = &operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i >= operand
	})
	return d
}

func (d *intFilter) Lte(operand int64) *intFilter {
	d.maxValue = &operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i <= operand
	})
	return d
}

func (d *intFilter) Gt(operand int64) *intFilter {
	d.minValue = &operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i > operand
	})
	return d
}

func (d *intFilter) Lt(operand int64) *intFilter {
	d.maxValue = &operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i < operand
	})
	return d
}

func (d *intFilter) End() Int {
	return Int{
		filter: *d,
	}
}

type Double struct {
	Data   float64 `parquet:"data"`
	filter doubleFilter
}

func NewDouble(value float64) Double {
	return Double{Data: value}
}

type doubleFilter struct {
	maxValue  *float64
	minValue  *float64
	filterOps []func(float64) bool
}

func (d *doubleFilter) max() any {
	return d.maxValue
}

func (d *doubleFilter) min() any {
	return d.minValue
}

func (d *doubleFilter) filter(v parquet.Value) bool {
	if v.Kind() != parquet.Double {
		return true
	}
	for _, op := range d.filterOps {
		if !op(v.Double()) {
			return false
		}
	}
	return true
}

func NewDoubleFilter() *doubleFilter {
	return new(doubleFilter)
}

func (d *doubleFilter) Gte(operand float64) *doubleFilter {
	d.minValue = &operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i >= operand
	})
	return d
}

func (d *doubleFilter) Lte(operand float64) *doubleFilter {
	d.maxValue = &operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i <= operand
	})
	return d
}

func (d *doubleFilter) Gt(operand float64) *doubleFilter {
	d.minValue = &operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i > operand
	})
	return d
}

func (d *doubleFilter) Lt(operand float64) *doubleFilter {
	d.maxValue = &operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i < operand
	})
	return d
}

func (d *doubleFilter) End() Double {
	return Double{
		filter: *d,
	}
}

type String struct {
	Data   string `parquet:"data"`
	filter stringFilter
}

func NewString(value string) String {
	return String{Data: value}
}

type stringFilter struct {
	filterOps []func(string) bool
}

func StringFilter() *stringFilter {
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
	return String{
		filter: *f,
	}
}
