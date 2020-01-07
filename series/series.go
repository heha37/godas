package series

import (
	"fmt"

	"github.com/heha37/godas/element"
)

type Series struct {
	elements element.Elements
	dataType element.Type
}

func (se *Series) String() string {
	return fmt.Sprint(se.elements)
}

func (se *Series) Len() int {
	return se.elements.Len()
}

func (se *Series) InitElements(dataType element.Type,size int) {
	switch dataType {
	case element.TypeInt:
		se.elements = make(element.ElementsInt, size)
		se.elements.Init(size)
	case element.TypeBool:
		se.elements = make(element.ElementsBool, size)
		se.elements.Init(size)
	case element.TypeString:
		se.elements = make(element.ElementsString, size)
		se.elements.Init(size)
	default:
	}
}

func (se *Series) Copy() (newSeries *Series) {
	newSeries = new(Series)
	dataType := se.dataType

	var elements element.Elements
	switch dataType {
	case element.TypeInt:
		elements = make(element.ElementsInt, se.Len())
		copy(elements.(element.ElementsInt), se.elements.(element.ElementsInt))
	case element.TypeBool:
		elements = make(element.ElementsBool, se.Len())
		copy(elements.(element.ElementsBool), se.elements.(element.ElementsBool))
	case element.TypeString:
		elements = make(element.ElementsString, se.Len())
		copy(elements.(element.ElementsString), se.elements.(element.ElementsString))
	}

	return &Series{
		elements: elements,
		dataType: dataType,
	}
}

func New(values interface{}) *Series {
	se := new(Series)

	switch values.(type) {
	case []int:
		vals := values.([]int)
		size := len(vals)
		se.InitElements(element.TypeInt, size)
		for i:=0; i<size; i++ {
			se.elements.Index(i).Set(vals[i])
		}
	case []bool:
		vals := values.([]bool)
		size := len(vals)
		se.InitElements(element.TypeBool, size)
		for i:=0; i<size; i++ {
			se.elements.Index(i).Set(vals[i])
		}
	case []string:
		vals := values.([]string)
		size := len(vals)
		se.InitElements(element.TypeString, size)
		for i:=0; i<size; i++ {
			se.elements.Index(i).Set(vals[i])
		}
	default:
	}

	return se
}
