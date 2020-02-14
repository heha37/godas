package elements_float

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementFloat32 = float32
type ElementsFloat32 []ElementFloat32

func (elements ElementsFloat32) Type() (sType types.Type) {
	return types.TypeFloat32
}

func (elements ElementsFloat32) Len() (sLen int) {
	return len(elements)
}

func (elements ElementsFloat32) String() string {
	return fmt.Sprint(elements)
}

func (elements ElementsFloat32) Copy() (newElements elements.Elements) {
	newSlice := make([]ElementFloat32, elements.Len())
	copy(newSlice, elements)

	newElements = ElementsFloat32(newSlice)
	return
}

func (elements ElementsFloat32) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_float32 size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]ElementFloat32, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = ElementsFloat32(newSlice)
	return
}