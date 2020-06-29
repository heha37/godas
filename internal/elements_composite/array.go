package elements_composite

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
	"github.com/hunknownz/godas/types"
	"reflect"
)

type Array struct {
	FieldName string
	Elements elements.Elements
}

func (array *Array) Copy() (newArray *Array) {
	newElements := array.Elements.Copy()
	newArray = &Array{
		FieldName: array.FieldName,
		Elements: newElements,
	}
	return
}

func (array *Array) Type() types.Type {
	return array.Elements.Type()
}

func (array *Array) Len() int {
	return array.Elements.Len()
}

func (array *Array) Subset(index index.IndexInt) (newSeries *Array, err error) {
	newElements, err := array.Elements.Subset(index)
	if err != nil {
		err = fmt.Errorf("subset series error: %w", err)
	}
	newSeries = &Array{
		FieldName: array.FieldName,
		Elements: newElements,
	}
	return
}

func (array *Array) IsNaN() []bool {
	return array.Elements.IsNaN()
}

func (array *Array) IsCondition(cond *condition.Condition) (ixs index.IndexBool, err error) {
	expr := cond.Prepare()
	seLen := array.Elements.Len()
	ixs = make(index.IndexBool, seLen)
	for i := 0; i < seLen; i++ {
		element, e := array.Elements.Location(i)
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

func (array *Array) Filter(cond *condition.Condition) (newArray *Array, err error) {
	ixs, err := array.IsCondition(cond)
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
	newArray, err = array.Subset(idx)
	if err != nil {
		err = fmt.Errorf("filter error: %w", err)
		return
	}
	return
}

func (array *Array) At(coord int) (value elements.ElementValue, err error) {
	return array.Elements.Location(coord)
}

func (array *Array) Swap(i, j int) {
	array.Elements.Swap(i, j)
}

func (array *Array) Append(copy bool, values ...interface{}) (newArray *Array, err error) {
	newElements, err := array.Elements.Append(copy, values)
	if err != nil {
		err = fmt.Errorf("append array error: %w", err)
		return
	}

	if !copy {
		newArray = array
	} else {
		newArray = &Array{
			FieldName: array.FieldName,
			Elements: newElements,
		}
	}
	return
}

func NewArray(values interface{}, fieldName string) (array *Array, err error) {
	array = new(Array)
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

	return
}