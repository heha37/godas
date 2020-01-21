package element

import (
	"errors"
	"fmt"
	"github.com/heha37/godas/utils"
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
	case Element:
		val, err := value.(Element).Bool()
		if err != nil {
			elem.nan = true
			return
		}
		elem.value = val
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

func (elem *elementBool) Bool() (val bool, err error) {
	if elem.IsNaN() {
		err = errors.New("cannot convert NaN to bool")
		return
	}
	val = elem.value
	return
}

func (elem *elementBool) Int() (val int, err error) {
	if elem.IsNaN() {
		err = errors.New("cannot convert NaN to Int")
	}
	val = utils.If(elem.value == true, 1, 0).(int)
	return
}

func (elem *elementBool) IsNaN() bool {
	return elem.nan
}

func (elem *elementBool) Copy() Element {
	if elem.IsNaN() {
		return &elementBool{
			nan: true,
		}
	}
	return &elementBool{
		value: elem.value,
		nan: false,
	}
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