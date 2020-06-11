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

const (
	ElementNaNString ElementString = "NaN"
)

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

func (elements ElementsString) IsNaN() []bool {
	elementsLen := elements.Len()
	nanElements := make([]bool, elementsLen)
	for i := 0; i < elementsLen; i++ {
		isNaN := elements[i] == ElementNaNString
		nanElements[i] = isNaN
	}
	return nanElements
}

func (elements ElementsString) Location(coord int) (element elements.ElementValue, err error) {
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	stringLen := elements.Len()
	if coord >= stringLen {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, stringLen))
		return
	}
	element.Value = elements[coord]
	element.Type = types.TypeString
	return
}

func (elements ElementsString) Swap(i, j int) {
	elements[i], elements[j] = elements[j], elements[i]
}