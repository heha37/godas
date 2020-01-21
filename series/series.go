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
	size := se.Len()

	switch dataType {
	case element.TypeInt:
		newSeries.InitElements(element.TypeInt, size)
	case element.TypeBool:
		newSeries.InitElements(element.TypeBool, size)
	case element.TypeString:
		newSeries.InitElements(element.TypeString, size)
	}
	for i:=0; i<size; i++ {
		val := se.elements.Index(i).Copy()
		newSeries.elements.Index(i).Set(val)
	}
	newSeries.dataType = dataType
	return
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
		se.dataType = element.TypeInt
	case []bool:
		vals := values.([]bool)
		size := len(vals)
		se.InitElements(element.TypeBool, size)
		for i:=0; i<size; i++ {
			se.elements.Index(i).Set(vals[i])
		}
		se.dataType = element.TypeBool
	case []string:
		vals := values.([]string)
		size := len(vals)
		se.InitElements(element.TypeString, size)
		for i:=0; i<size; i++ {
			se.elements.Index(i).Set(vals[i])
		}
		se.dataType = element.TypeString
	default:
	}

	return se
}
