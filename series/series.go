package series

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/condition"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	sbool "github.com/hunknownz/godas/internal/elements_bool"
	sfloat "github.com/hunknownz/godas/internal/elements_float"
	sint "github.com/hunknownz/godas/internal/elements_int"
	sobject "github.com/hunknownz/godas/internal/elements_object"
	sstring "github.com/hunknownz/godas/internal/elements_string"
	"github.com/hunknownz/godas/order"
	"github.com/hunknownz/godas/types"
	"reflect"
)

type Series struct {
	FieldName string
	elements elements.Elements
}

func (se *Series) Copy() (newSeries *Series) {
	newElements := se.elements.Copy()
	newSeries = &Series{
		FieldName: se.FieldName,
		elements:newElements,
	}
	return
}

func (se *Series) Type() types.Type {
	return se.elements.Type()
}

func (se *Series) Len() int {
	return se.elements.Len()
}

func (se *Series) Subset(index index.IndexInt) (newSeries *Series, err error) {
	newElements, err := se.elements.Subset(index)
	if err != nil {
		err = fmt.Errorf("subset series error: %w", err)
	}
	newSeries = &Series{
		FieldName: se.FieldName,
		elements:newElements,
	}
	return
}

func (se *Series) IsNaN() []bool {
	return se.elements.IsNaN()
}

func (se *Series) IsCondition(cond *condition.Condition) (ixs index.IndexBool, err error) {
	expr := cond.Prepare()
	seLen := se.elements.Len()
	ixs = make(index.IndexBool, seLen)
	for i := 0; i < seLen; i++ {
		element, e := se.elements.Location(i)
		if e != nil {
			err = fmt.Errorf("is condition error: %w", e)
			return
		}
		ixs[i] = element.EvaluateCondition(expr)
		if element.Err != nil {
			err = element.Err
			return
		}
	}
	return ixs, nil
}

func (se *Series) Filter(cond *condition.Condition) (newSeries *Series, err error) {
	ixs, err := se.IsCondition(cond)
	if err != nil {
		err = fmt.Errorf("filter error: %w", err)
		return
	}
	idx := make(index.IndexInt, 0)
	for ix, ixVal := range ixs {
		if ixVal {
			idx = append(idx, uint32(ix))
		}
	}
	newSeries, err = se.Subset(idx)
	if err != nil {
		err = fmt.Errorf("filter error: %w", err)
		return
	}
	return
}

func (se *Series) At(coord int) (value elements.ElementValue, err error) {
	return se.elements.Location(coord)
}

func NewCondition() *condition.Condition {
	return condition.NewCondition(condition.ConditionTypeSeries)
}

func (se *Series) NewIntLessFunc(f order.IntLessFunc) order.LessFunc {
	elements := se.elements.(sint.ElementsInt64)
	return func(i, j int) bool {
		return f(elements[i], elements[j])
	}
}

func (se *Series) defaultIntLessFunc() order.LessFunc {
	elements := se.elements.(sint.ElementsInt64)
	return func(i, j int) bool {
		return elements[i] < elements[j]
	}
}

func (se *Series) NewFloatLessFunc(f order.FloatLessFunc) order.LessFunc {
	elements := se.elements.(sfloat.ElementsFloat64)
	return func(i, j int) bool {
		return f(elements[i], elements[j])
	}
}

func (se *Series) NewStringLessFunc(f order.StringLessFunc) order.LessFunc {
	elements := se.elements.(sstring.ElementsString)
	return func(i, j int) bool {
		return f(elements[i], elements[j])
	}
}

func (se *Series) NewBoolLessFunc(f order.BoolLessFunc) order.LessFunc {
	elements := se.elements.(sbool.ElementsBool)
	return func(i, j int) bool {
		a, _ := elements.Location(i)
		b, _ := elements.Location(j)
		return f(a.MustBool(), b.MustBool())
	}
}

func (se *Series) defaultBoolLessFunc() order.LessFunc {
	elements := se.elements.(sbool.ElementsBool)
	return func(i, j int) bool {
		a, _ := elements.Location(i)
		b, _ := elements.Location(j)
		aValue := a.MustBool()
		bValue := b.MustBool()
		return !aValue && bValue
	}
}

func (se *Series) Sort(inPlace bool, ascending bool, orderBy ...order.LessFunc) (newSe *Series, err error) {
	if inPlace {
		newSe = se
	} else {
		newSe = &Series{
			FieldName: se.FieldName,
			elements:  se.elements,
		}
		se.elements = se.elements.Copy()
	}

	var f order.LessFunc
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
	se.elements.Swap(i, j)
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

	newSe.elements, _ = newSe.elements.Append(copy, records...)
	return
}

func New(values interface{}, fieldName string) (se *Series, err error) {
	switch values.(type) {
	case []int:
		vals := values.([]int)
		valsLen := len(vals)
		vals64 := make([]int64, valsLen)
		for i := 0; i < valsLen; i++ {
			vals64[i] = int64(vals[i])
		}
		newElements := sint.NewElementsInt64(vals64)
		se = &Series{elements:newElements}
	case []int64:
		vals := values.([]int64)
		newElements := sint.NewElementsInt64(vals)
		se = &Series{elements:newElements}
	case []bool:
		vals := values.([]bool)
		newElements := sbool.NewElementsBool(vals)
		se = &Series{elements:newElements}
	case []string:
		vals := values.([]string)
		newElements := sstring.NewElementsString(vals)
		se = &Series{elements:newElements}
	case []float32:
		vals := values.([]float32)
		valsLen := len(vals)
		vals64 := make([]float64, valsLen)
		for i := 0; i < valsLen; i++ {
			vals64[i] = float64(vals[i])
		}
		newElements := sfloat.NewElementsFloat64(vals64)
		se = &Series{elements:newElements}
	case []float64:
		vals := values.([]float64)
		newElements := sfloat.NewElementsFloat64(vals)
		se = &Series{elements:newElements}
	case []interface{}:
		vals := values.([]interface{})
		newElements := sobject.NewElementsObject(vals)
		se = &Series{elements:newElements}
	default:
		typ := reflect.TypeOf(values).Kind().String()
		err = errors.New(fmt.Sprintf("new series errors: type %s is not supported", typ))
		return
	}

	se.FieldName = fieldName

	return
}