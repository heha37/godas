package godas

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/condition"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	sbool "github.com/hunknownz/godas/internal/elements_bool"
	"github.com/hunknownz/godas/internal/elements_composite"
	sfloat "github.com/hunknownz/godas/internal/elements_float"
	sint "github.com/hunknownz/godas/internal/elements_int"
	sobject "github.com/hunknownz/godas/internal/elements_object"
	sstring "github.com/hunknownz/godas/internal/elements_string"
	"github.com/hunknownz/godas/types"
	"reflect"
)

type Series struct {
	array *elements_composite.Array
}

func (se *Series) Copy() (newSeries *Series) {
	newSeries = &Series{
		array: se.array.Copy(),
	}
	return
}

func (se *Series) Type() types.Type {
	return se.array.Type()
}

func (se *Series) Len() int {
	return se.array.Len()
}

func (se *Series) Subset(index index.IndexInt) (newSeries *Series, err error) {
	array, err := se.array.Subset(index)
	if err != nil {
		err = fmt.Errorf("subset series error: %w", err)
	}
	newSeries = &Series{
		array,
	}
	return
}

func (se *Series) IsNaN() []bool {
	return se.array.IsNaN()
}

func (se *Series) IsCondition(cond *condition.Condition) (ixs index.IndexBool, err error) {
	return se.array.IsCondition(cond)
}

func (se *Series) Filter(cond *condition.Condition) (newSeries *Series, err error) {
	array, e := se.array.Filter(cond)
	if e != nil {
		err = fmt.Errorf("series filter error: %w", e)
		return
	}
	newSeries = &Series{
		array,
	}
	return
}

func (se *Series) At(coord int) (value elements.ElementValue, err error) {
	return se.array.At(coord)
}

func NewSeriesCondition() *condition.Condition {
	return condition.NewCondition(condition.ConditionTypeSeries)
}

func (se *Series) NewIntLessFunc(f IntLessFunc) LessFunc {
	elements := se.array.Elements.(sint.ElementsInt64)
	return func(i, j int) bool {
		return f(elements[i], elements[j])
	}
}

func (se *Series) defaultIntLessFunc() LessFunc {
	elements := se.array.Elements.(sint.ElementsInt64)
	return func(i, j int) bool {
		return elements[i] < elements[j]
	}
}

func (se *Series) NewFloatLessFunc(f FloatLessFunc) LessFunc {
	elements := se.array.Elements.(sfloat.ElementsFloat64)
	return func(i, j int) bool {
		return f(elements[i], elements[j])
	}
}

func (se *Series) NewStringLessFunc(f StringLessFunc) LessFunc {
	elements := se.array.Elements.(sstring.ElementsString)
	return func(i, j int) bool {
		return f(elements[i], elements[j])
	}
}

func (se *Series) NewBoolLessFunc(f BoolLessFunc) LessFunc {
	elements := se.array.Elements.(sbool.ElementsBool)
	return func(i, j int) bool {
		a, _ := elements.Location(i)
		b, _ := elements.Location(j)
		return f(a.MustBool(), b.MustBool())
	}
}

func (se *Series) defaultBoolLessFunc() LessFunc {
	elements := se.array.Elements.(sbool.ElementsBool)
	return func(i, j int) bool {
		a, _ := elements.Location(i)
		b, _ := elements.Location(j)
		aValue := a.MustBool()
		bValue := b.MustBool()
		return !aValue && bValue
	}
}

func (se *Series) Sort(inPlace bool, ascending bool, orderBy ...LessFunc) (newSe *Series, err error) {
	if inPlace {
		newSe = se
	} else {
		newSe = se.Copy()
	}

	var f LessFunc
	if len(orderBy) > 0 {
		f = orderBy[0]
	} else {
		typ := newSe.Type()
		switch typ {
		case types.TypeInt:
			f = newSe.defaultIntLessFunc()
		case types.TypeBool:
			f = newSe.defaultBoolLessFunc()
		}
	}
	sorter := newSeriesSorter(newSe, ascending, f)
	sorter.Sort()

	return
}

func (se *Series) Swap(i, j int) {
	se.array.Swap(i, j)
}

func (se *Series) Append(copy bool, records ...interface{}) (newSe *Series, err error) {
	if !copy {
		newSe = se
	} else {
		newSe = se.Copy()
	}

	seType := se.Type()
	for _, record := range records {
		kind := reflect.TypeOf(record).Kind()
		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if seType != types.TypeInt {
				err = errors.New(fmt.Sprintf("%s series can't append %s value", types.TypeInt, kind.String()))
				return
			}
		case reflect.Float32, reflect.Float64:
			if seType != types.TypeFloat {
				err = errors.New(fmt.Sprintf("%s series can't append %s value", types.TypeFloat, kind.String()))
				return
			}
		case reflect.String:
			if seType != types.TypeString {
				err = errors.New(fmt.Sprintf("%s series can't append %s value", types.TypeString, kind.String()))
				return
			}
		case reflect.Bool:
			if seType != types.TypeBool {
				err = errors.New(fmt.Sprintf("%s series can't append %s value", types.TypeBool, kind.String()))
				return
			}
		case reflect.Interface:
			if seType != types.TypeObject {
				err = errors.New(fmt.Sprintf("%s series can't append %s value", types.TypeObject, kind.String()))
				return
			}
		default:
			err = errors.New(fmt.Sprintf("%s is not supported to append", kind.String()))
		}
	}

	newSe.array, _ = newSe.array.Append(copy, records...)
	return
}

func NewSeries(values interface{}, fieldName string) (se *Series, err error) {
	array := new(elements_composite.Array)
	switch values.(type) {
	case []int:
		vals := values.([]int)
		valsLen := len(vals)
		vals64 := make([]int64, valsLen)
		for i := 0; i < valsLen; i++ {
			vals64[i] = int64(vals[i])
		}
		newElements := sint.NewElementsInt64(vals64)
		array.Elements = newElements
	case []int64:
		vals := values.([]int64)
		newElements := sint.NewElementsInt64(vals)
		array.Elements = newElements
	case []bool:
		vals := values.([]bool)
		newElements := sbool.NewElementsBool(vals)
		array.Elements = newElements
	case []string:
		vals := values.([]string)
		newElements := sstring.NewElementsString(vals)
		array.Elements = newElements
	case []float32:
		vals := values.([]float32)
		valsLen := len(vals)
		vals64 := make([]float64, valsLen)
		for i := 0; i < valsLen; i++ {
			vals64[i] = float64(vals[i])
		}
		newElements := sfloat.NewElementsFloat64(vals64)
		array.Elements = newElements
	case []float64:
		vals := values.([]float64)
		newElements := sfloat.NewElementsFloat64(vals)
		array.Elements = newElements
	case []interface{}:
		vals := values.([]interface{})
		newElements := sobject.NewElementsObject(vals)
		array.Elements = newElements
	default:
		typ := reflect.TypeOf(values).Kind().String()
		err = errors.New(fmt.Sprintf("new series errors: type %s is not supported", typ))
		return
	}
	array.FieldName = fieldName

	se = &Series{
		array: array,
	}
	return
}