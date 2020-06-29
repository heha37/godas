package godas

type SortKey struct {
	Column    string
	Ascending bool
	LessFunc  LessFunc
}

type LessFunc func(i, j int) bool

type IntLessFunc func(e1, e2 int64) bool
type StringLessFunc func(e1, e2 string) bool
type FloatLessFunc func(e1, e2 float64) bool
type BoolLessFunc func(e1, e2 bool) bool
type ObjectLessFunc func(e1, e2 interface{}) bool