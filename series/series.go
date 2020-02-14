package series

import (
	"fmt"
	"github.com/hunknownz/godas/index"
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