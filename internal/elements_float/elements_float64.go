package elements_float

import (
	"errors"
	"fmt"
	"math"
	"reflect"

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

func (elements ElementsFloat64) Swap(i, j int) {
	elements[i], elements[j] = elements[j], elements[i]
}

func (elements ElementsFloat64) Append(copy bool, values ...interface{}) (newElements elements.Elements, err error) {
	var nElements ElementsFloat64
	if !copy {
		nElements = elements
	} else {
		nElements = elements.Copy().(ElementsFloat64)
	}

	for _, value := range values {
		kind := reflect.TypeOf(value).Kind()
		if kind != reflect.Float32 && kind != reflect.Float64 {
			err = errors.New(fmt.Sprintf("float elements can't append %s", kind.String()))
			return
		}
	}

	for _, value := range values {
		switch value.(type) {
		case float32:
			val := value.(float32)
			nElements = append(nElements, float64(val))
		case float64:
			val := value.(float64)
			nElements = append(nElements, val)
		}
	}
	newElements = nElements

	return
}