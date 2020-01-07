package element

import (
	"errors"
	"fmt"
)

type elementBool struct {
	value bool
	nan bool
}

func (elem *elementBool) Type() Type {
	return TypeBool
}

func (elem *elementBool) Set(value interface{}) {
	elem.nan = false
	switch value.(type) {
	case bool:
		elem.value = value.(bool)
	default:
		elem.nan = true
	}
}

func (elem *elementBool) String() string {
	if elem.nan {
		return NaN
	}
	return fmt.Sprint(elem.value)
}

type ElementsBool []*elementBool

func (elems ElementsBool) Len() int {
	return len(elems)
}

func (elems ElementsBool) Init(size int) {
	if size > elems.Len() {
		msg := fmt.Sprintf("size %d is out of elements size %d", size, elems.Len())
		panic(errors.New(msg))
	}
	for i := 0; i < size; i++ {
		elems[i] = new(elementBool)
	}
}

func (elems ElementsBool) Index(i int) Element {
	return elems[i]
}