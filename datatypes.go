package lakedb

type Double struct {
	data       float64
	max, min   float64
	operations []func(float64) bool
}

func (f *Double) Bounds() (float64, float64) {
	return f.min, f.max
}

func (f *Double) Check(operand float64) bool {
	for _, op := range f.operations {
		if !op(operand) {
			return false
		}
	}
	return true
}

func (f *Double) Gt(operand float64) *Double {
	f.operations = append(f.operations, func(value float64) bool {
		return value > operand
	})
	return f
}
