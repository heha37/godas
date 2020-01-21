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
	case Element:
		val, err := value.(Element).Int()
		if err != nil {
			elem.nan = true
			return
		}
		elem.value = val
	default:
		elem.nan = true
	}
	return
}

func (elem *elementInt) String() string {
	if elem.nan {
		return NaN
	}
	return fmt.Sprint(elem.value)
}

func (elem *elementInt) Int() (val int, err error) {
	if elem.IsNaN() {
		err = errors.New("cannot convert NaN to int")
		return
	}
	val = elem.value
	return
}

func (elem *elementInt) Bool() (val bool, err error) {
	if elem.IsNaN() {
		err = errors.New("cannot convert NaN to bool")
		return
	}
	switch elem.value {
	case 0:
		val = false
		return
	case 1:
		val = true
		return
	}
	err = errors.New(fmt.Sprintf("cannot convert %d to bool", elem.value))
	return
}

func (elem *elementInt) IsNaN() bool {
	return elem.nan
}

func (elem *elementInt) Copy() Element {
	if elem.IsNaN() {
		return &elementInt{
			nan: true,
		}
	}
	return &elementInt{
		value: elem.value,
		nan: false,
	}
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