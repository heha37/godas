package elements_object

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementObject = interface{}
type ElementsObject struct {
	itemsLen int
	items []ElementObject
}

func (elements ElementsObject) Type() (sType types.Type) {
	return types.TypeInt
}

func (elements ElementsObject) Len() (sLen int) {
	return elements.itemsLen
}

func (elements ElementsObject) String() string {
	return fmt.Sprint(elements.items)
}

func (elements ElementsObject) Copy() (newElements elements.Elements) {
	var newSlice []ElementObject
	if len(elements.items) == 0 {
		newSlice = []ElementObject{}
	} else {
		newSlice = make([]ElementObject, elements.Len())
		copy(newSlice, elements.items)
	}

	newElements = ElementsObject{
		itemsLen: elements.Len(),
		items: newSlice,
	}
	return
}

func (elements ElementsObject) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_object size %d", idxLen, elements.Len()))
		return
	}

	var newSlice []ElementObject
	if len(elements.items) == 0 {
		newSlice = []ElementObject{}
	} else {
		newSlice = make([]ElementObject, idxLen)
		for newElementsI, indexI := range idx {
			newSlice[newElementsI] = elements.items[indexI]
		}
	}

	newElements = ElementsObject{
		itemsLen: idxLen,
		items: newSlice,
	}
	return
}

func (elements ElementsObject) IsNaN() []bool {
	elementsLen := elements.Len()
	nanElements := make([]bool, elementsLen)
	if len(elements.items) == 0 {
		return nanElements
	}
	for i := 0; i < elementsLen; i++ {
		isNaN := elements.items[i] == nil
		nanElements[i] = isNaN
	}
	return nanElements
}

func (elements ElementsObject) Location(coord int) (element elements.ElementValue, err error) {
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	objectLen := elements.Len()
	if coord >= objectLen {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, objectLen))
		return
	}
	if len(elements.items) == 0 {
		element.Value = interface{}(nil)
	} else {
		element.Value = elements.items[coord]
	}
	element.Type = types.TypeObject
	return
}

func (elements ElementsObject) Swap(i, j int) {
	elements.items[i], elements.items[j] = elements.items[j], elements.items[i]
}