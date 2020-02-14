package elements_string

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementString = string
type ElementsString []ElementString

func (elements ElementsString) Type() (sType types.Type) {
	return types.TypeString
}

func (elements ElementsString) Len() (sLen int) {
	return len(elements)
}

func (elements ElementsString) String() string {
	return fmt.Sprint(elements)
}

func (elements ElementsString) Copy() (newElements elements.Elements) {
	newSlice := make([]ElementString, elements.Len())
	copy(newSlice, elements)

	newElements = ElementsString(newSlice)
	return
}

func (elements ElementsString) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_string size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]ElementString, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = ElementsString(newSlice)
	return
}