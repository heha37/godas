package elements_int

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type Elements []int

func (elements Elements) Type() (sType types.Type) {
	return types.TypeInt
}

func (elements Elements) Len() (sLen int) {
	return len(elements)
}

func (elements Elements) String() string {
	return fmt.Sprint(elements)
}

func (elements Elements) Copy() (newElements elements.Elements) {
	newSlice := make([]int, elements.Len())
	copy(newSlice, elements)

	newElements = Elements(newSlice)
	return
}

func (elements Elements) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_int size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]int, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = Elements(newSlice)
	return
}

func New(elements []int) (newElements Elements) {
	newElements = elements
	return
}