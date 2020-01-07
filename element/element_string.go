package element

import (
	"errors"
	"fmt"
)

type elementString struct {
	value string
	nan bool
}

func (elem *elementString) Type() Type {
	return TypeString
}

func (elem *elementString) Set(value interface{}) {
	elem.nan = false
	switch value.(type) {
	case string:
		elem.value = value.(string)
	default:
		elem.nan = true
	}
}

func (elem *elementString) String() string {
	if elem.nan {
		return NaN
	}
	return fmt.Sprint(elem.value)
}

type ElementsString []*elementString

func (elems ElementsString) Len() int {
	return len(elems)
}

func (elems ElementsString) Init(size int) {
	if size > elems.Len() {
		msg := fmt.Sprintf("size %d is out of elements size %d", size, elems.Len())
		panic(errors.New(msg))
	}
	for i := 0; i < size; i++ {
		elems[i] = new(elementString)
	}
}

func (elems ElementsString) Index(i int) Element {
	return elems[i]
}