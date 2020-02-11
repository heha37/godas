package series

import (
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	sbool "github.com/hunknownz/godas/internal/elements_bool"
	sint "github.com/hunknownz/godas/internal/elements_int"
)

type Series struct {
	elements elements.Elements
}

func (se *Series) Copy() (newSeries *Series) {
	newElements := se.elements.Copy()
	newSeries = &Series{elements:newElements}
	return
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

func New(values interface{}) (se *Series) {

	switch values.(type) {
	case []int:
		vals := values.([]int)
		newElements := sint.New(vals)
		se = &Series{elements:newElements}
	case []bool:
		vals := values.([]bool)
		newElements := sbool.New(vals)
		se = &Series{elements:newElements}
	case []string:
		//vals := values.([]string)
	default:
	}

	return se
}