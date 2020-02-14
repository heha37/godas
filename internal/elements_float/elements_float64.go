package elements_float

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementFloat64 = float64
type ElementsFloat64 []ElementFloat64

func (elements ElementsFloat64) Type() (sType types.Type) {
	return types.TypeFloat64
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