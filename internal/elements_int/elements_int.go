package elements_int

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementInt = int
type ElementsInt []ElementInt

func (elements ElementsInt) Type() (sType types.Type) {
	return types.TypeInt
}

func (elements ElementsInt) Len() (sLen int) {
	return len(elements)
}

func (elements ElementsInt) String() string {
	return fmt.Sprint(elements)
}

func (elements ElementsInt) Copy() (newElements elements.Elements) {
	newSlice := make([]ElementInt, elements.Len())
	copy(newSlice, elements)

	newElements = ElementsInt(newSlice)
	return
}

func (elements ElementsInt) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_int size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]ElementInt, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = ElementsInt(newSlice)
	return
}