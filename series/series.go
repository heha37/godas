package series

import (
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/condition"
	"github.com/hunknownz/godas/internal/elements"
	sbool "github.com/hunknownz/godas/internal/elements_bool"
	sint "github.com/hunknownz/godas/internal/elements_int"
	sstring "github.com/hunknownz/godas/internal/elements_string"
	sfloat "github.com/hunknownz/godas/internal/elements_float"
	"github.com/hunknownz/godas/types"
)

type Series struct {
	elements elements.Elements
}

func (se *Series) Copy() (newSeries *Series) {
	newElements := se.elements.Copy()
	newSeries = &Series{elements:newElements}
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
	newSeries = &Series{elements:newElements}
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

func NewCondition() *condition.Condition {
	return condition.NewCondition(condition.ConditionTypeSeries)
}

func New(values interface{}) (se *Series) {
	switch values.(type) {
	case []int:
		vals := values.([]int)
		newElements := sint.NewElementsInt(vals)
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
	default:

	}

	return se
}