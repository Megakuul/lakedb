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

var _ genericFilter = &Int{}

type Int struct {
	intFilter
	Data int64 `parquet:"data"`
}

func NewInt(value int64) Int {
	return Int{Data: value}
}

type intFilter struct {
	maxValue, minValue any
	filterOps          []func(int64) bool
}

func (d intFilter) max() any {
	return d.maxValue
}

func (d intFilter) min() any {
	return d.minValue
}

func (d intFilter) filter(v parquet.Value) bool {
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
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i >= operand
	})
	return d
}

func (d *intFilter) Lte(operand int64) *intFilter {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i <= operand
	})
	return d
}

func (d *intFilter) Gt(operand int64) *intFilter {
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i > operand
	})
	return d
}

func (d *intFilter) Lt(operand int64) *intFilter {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i int64) bool {
		return i < operand
	})
	return d
}

func (d *intFilter) End() Int {
	return Int{*d, 0}
}

type Double struct {
	doubleFilter
	Data float64 `parquet:"data"`
}

func NewDouble(value float64) Double {
	return Double{Data: value}
}

type doubleFilter struct {
	maxValue, minValue any
	filterOps          []func(float64) bool
}

func (d doubleFilter) max() any {
	return d.maxValue
}

func (d doubleFilter) min() any {
	return d.minValue
}

func (d doubleFilter) filter(v parquet.Value) bool {
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
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i >= operand
	})
	return d
}

func (d *doubleFilter) Lte(operand float64) *doubleFilter {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i <= operand
	})
	return d
}

func (d *doubleFilter) Gt(operand float64) *doubleFilter {
	d.minValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i > operand
	})
	return d
}

func (d *doubleFilter) Lt(operand float64) *doubleFilter {
	d.maxValue = operand
	d.filterOps = append(d.filterOps, func(i float64) bool {
		return i < operand
	})
	return d
}

func (d *doubleFilter) End() Double {
	return Double{*d, 0}
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
