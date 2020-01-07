package element

import (
	"errors"
	"fmt"
)

type elementInt struct {
	value int
	nan bool
}

func (elem *elementInt) Type() Type {
	return TypeInt
}

func (elem *elementInt) Set(value interface{}) {
	elem.nan = false
	switch value.(type) {
	case int:
		elem.value = value.(int)
	default:
		elem.nan = true
	}
}

func (elem *elementInt) String() string {
	if elem.nan {
		return NaN
	}
	return fmt.Sprint(elem.value)
}

type ElementsInt []*elementInt

func (elems ElementsInt) Len() int {
	return len(elems)
}

func (elems ElementsInt) Init(size int) {
	if size > elems.Len() {
		msg := fmt.Sprintf("size %d is out of elements size %d", size, elems.Len())
		panic(errors.New(msg))
	}
	for i := 0; i < size; i++ {
		elems[i] = new(elementInt)
	}
}

func (elems ElementsInt) Index(i int) Element {
	return elems[i]
}