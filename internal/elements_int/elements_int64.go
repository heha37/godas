package elements_int

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementInt64 = int64
type ElementsInt64 []ElementInt64

func (elements ElementsInt64) Type() (sType types.Type) {
	return types.TypeInt64
}

func (elements ElementsInt64) Len() (sLen int) {
	return len(elements)
}

func (elements ElementsInt64) String() string {
	return fmt.Sprint(elements)
}

func (elements ElementsInt64) Copy() (newElements elements.Elements) {
	newSlice := make([]ElementInt64, elements.Len())
	copy(newSlice, elements)

	newElements = ElementsInt64(newSlice)
	return
}

func (elements ElementsInt64) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_int64 size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]ElementInt64, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = ElementsInt64(newSlice)
	return
}