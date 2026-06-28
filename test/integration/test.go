package integration

type RequestMetric struct {
	Timestamp      int64
	Latency        float64
	CriticalErrors int64
	Skew           float64

	Endpoint string
}

type Filter struct {
	Int    *intFilter
	Double *doubleFilter
}

type intFilter struct {
	id     int64
	checks map[int64][]func(left int64) bool
}

func (f *intFilter) Eq(right int64) *intFilter {
	if f.id == 0 {
		f.id = 123 // random
	}
	f.checks[f.id] = append(f.checks[f.id], func(left int64) bool { return left == right })
	return f
}

func (f *intFilter) Commit() int64 {
	return f.id
}

type doubleFilter struct {
	id     float64
	checks map[float64][]func(left float64) bool
}

func (f *doubleFilter) Eq(right float64) *doubleFilter {
	if f.id == 0 {
		f.id = 123 // random
	}
	f.checks[f.id] = append(f.checks[f.id], func(left float64) bool { return left == right })
	return f
}

func (f *doubleFilter) Commit() float64 {
	return f.id
}

func Scan[T any](filter func(f Filter) T) {
}

func main() {
	Scan(func(f Filter) RequestMetric {
		return RequestMetric{
			Timestamp: f.Int.Eq(50).Commit(),
			Latency:   f.Double.Eq(5).Commit(),
		}
	})
}
