package element

import (
	"errors"
	"fmt"
	"github.com/heha37/godasd/element"
	"strconv"
	"strings"
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
		if elem.value == element.NaN {
			elem.nan = true
		}
	case Element:
		elem.value = value.(Element).String()
		if elem.value == element.NaN {
			elem.nan = true
		}
	default:
		elem.nan = true
	}
	return
}

func (elem *elementString) String() string {
	if elem.IsNaN() {
		return NaN
	}
	return elem.value
}

func (elem *elementString) Int() (val int, err error) {
	if elem.IsNaN() {
		err = errors.New("cannot convert NaN to int")
		return
	}
	val, err = strconv.Atoi(elem.value)
	return
}

func (elem *elementString) Bool() (val bool, err error) {
	if elem.IsNaN() {
		err = errors.New("cannot convert NaN to bool")
		return
	}

	switch strings.ToLower(elem.value) {
	case "true", "t", "1":
		val = true
		return
	case "false", "f", "0":
		val = false
		return
	}
	err = errors.New(fmt.Sprintf("cannot convert string %s to bool", elem.value))
	return
}

func (elem *elementString) IsNaN() bool {
	return elem.nan
}

func (elem *elementString) Copy() Element {
	if elem.IsNaN() {
		return &elementString{
			nan: true,
		}
	}
	return &elementString{
		value: elem.value,
		nan: false,
	}
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