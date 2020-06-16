package elements_int

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
	"math"
	"reflect"
)

type ElementInt64 = int64
type ElementsInt64 []ElementInt64

const (
	ElementNaNInt64 ElementInt64 = math.MinInt64
)

func (elements ElementsInt64) Type() (sType types.Type) {
	return types.TypeInt
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
		err = errors.New(fmt.Sprintf("index size %d off elements_int size %d", idxLen, elements.Len()))
		return
	}
	newSlice := make([]ElementInt64, idxLen)
	for newElementsI, indexI := range idx {
		newSlice[newElementsI] = elements[indexI]
	}

	newElements = ElementsInt64(newSlice)
	return
}

func (elements ElementsInt64) IsNaN() []bool {
	elementsLen := elements.Len()
	nanElements := make([]bool, elementsLen)
	for i := 0; i < elementsLen; i++ {
		isNaN := elements[i] == ElementNaNInt64
		nanElements[i] = isNaN
	}
	return nanElements
}

func (elements ElementsInt64) Location(coord int) (element elements.ElementValue, err error) {
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	int64Len := elements.Len()
	if coord >= int64Len {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, int64Len))
		return
	}
	element.Value = elements[coord]
	element.Type = types.TypeInt
	return
}

func (elements ElementsInt64) Swap(i, j int) {
	elements[i], elements[j] = elements[j], elements[i]
}

func (elements ElementsInt64) Append(copy bool, values ...interface{}) (newElements elements.Elements, err error) {
	var nElements ElementsInt64
	if !copy {
		nElements = elements
	} else {
		nElements = elements.Copy().(ElementsInt64)
	}

	for _, value := range values {
		kind := reflect.TypeOf(value).Kind()
		if kind != reflect.Int && kind != reflect.Int8 && kind != reflect.Int16 &&
			kind != reflect.Int32 && kind != reflect.Int64 {
			err = errors.New(fmt.Sprintf("int elements can't append %s", kind.String()))
			return
			}
	}

	for _, value := range values {
		switch value.(type) {
		case int:
			val := value.(int)
			nElements = append(nElements, int64(val))
		case int8:
			val := value.(int8)
			nElements = append(nElements, int64(val))
		case int16:
			val := value.(int16)
			nElements = append(nElements, int64(val))
		case int32:
			val := value.(int32)
			nElements = append(nElements, int64(val))
		case int64:
			val := value.(int64)
			nElements = append(nElements, val)
		}
	}
	newElements = nElements

	return
}