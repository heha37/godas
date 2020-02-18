package elements_float

import (
	"errors"
	"fmt"
	"math"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementFloat64 = float64
type ElementsFloat64 []ElementFloat64

func (elements ElementsFloat64) Type() (sType types.Type) {
	return types.TypeFloat
}

func (elements ElementsFloat64) Len() (sLen int) {
	return len(elements)
}

func (elements ElementsFloat64) String() string {
	return fmt.Sprint(elements)
}

func (elements ElementsFloat64) Copy() (newElements elements.Elements) {
	newSlice := make([]ElementFloat64, elements.Len())
	copy(newSlice, elements)

	newElements = ElementsFloat64(newSlice)
	return
}

func (elements ElementsFloat64) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_float64 size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]ElementFloat64, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = ElementsFloat64(newSlice)
	return
}

func (elements ElementsFloat64) IsNaN() []bool {
	elementsLen := elements.Len()
	nanElements := make([]bool, elementsLen)
	for i := 0; i < elementsLen; i++ {
		isNaN := math.IsNaN(elements[i])
		nanElements[i] = isNaN
	}
	return nanElements
}

func (elements ElementsFloat64) Location(coord int) (element elements.ElementValue, err error) {
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	float64Len := elements.Len()
	if coord >= float64Len {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, float64Len))
		return
	}
	element.Value = elements[coord]
	element.Type = types.TypeFloat
	return
}