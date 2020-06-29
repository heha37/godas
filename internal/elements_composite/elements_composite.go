package elements_composite

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
	"reflect"
	"strconv"
)

type ElementsComposite struct {
	NArray []*Array
	Fields []string
	FieldArraysMap map[string]int
}

func (els *ElementsComposite) Type() types.Type {
	return types.TypeComposite
}

func (els *ElementsComposite) String() string {
	return fmt.Sprint(els)
}

func (els *ElementsComposite) Len() int {
	return els.numRow()
}

func checkColumnsLengths(arrays ...*Array) (rows, cols int, err error) {
	cols = len(arrays)
	rows = -1
	if arrays == nil || cols == 0 {
		return
	}

	rows = arrays[0].Len()
	for i:=1; i<cols; i++ {
		if rows != arrays[i].Len() {
			err = errors.New("elements must all be same length")
			return
		}
	}
	return
}

func newFromArrays(arrays ...*Array) (newElements *ElementsComposite, err error) {
	if arrays == nil || len(arrays) == 0 {
		newElements = &ElementsComposite{
			NArray: make([]*Array, 0, 0),
			Fields: make([]string, 0, 0),
			FieldArraysMap: make(map[string]int),
		}
		return
	}

	_, colNum, err := checkColumnsLengths(arrays...)
	if err != nil {
		err = fmt.Errorf("new composite elements error: %w", err)
		return
	}

	nArray := make([]*Array, len(arrays))
	for i, array := range arrays {
		nArray[i] = array.Copy()
	}

	newElements = &ElementsComposite{
		NArray:   nArray,
		Fields: make([]string, colNum),
		FieldArraysMap: make(map[string]int),
	}

	for i := 0; i < colNum; i++ {
		key := nArray[i].FieldName
		if key == "" {
			key = "C" + strconv.Itoa(i)
			nArray[i].FieldName = key
		}
		newElements.Fields[i] = key
		newElements.FieldArraysMap[key] = i
	}

	return
}

func (els *ElementsComposite) Copy() (newElements elements.Elements) {
	newElements, _ = newFromArrays(els.NArray...)
	return
}

func (els *ElementsComposite) numRow() int {
	if len(els.NArray) != 0 {
		return els.NArray[0].Len()
	}
	return 0
}

func (els *ElementsComposite) numColumn() int {
	return len(els.Fields)
}

func (els *ElementsComposite) Subset(index index.IndexInt) (newElements elements.Elements, err error) {
	columnNum := els.numColumn()
	arrays := make([]*Array, columnNum)
	for i, array := range els.NArray {
		newArray, e := array.Subset(index)
		if e != nil {
			err = fmt.Errorf("sbuset dataframe error: %w", e)
			return
		}
		arrays[i] = newArray
	}

	_, colNum, err := checkColumnsLengths(arrays...)
	if err != nil {
		err = fmt.Errorf("subset dataframe error: %w", err)
		return
	}

	fields := make([]string, colNum)
	fieldArraysMap := make(map[string]int)
	for i, iField := range els.Fields {
		fields[i] = iField
		fieldArraysMap[iField] = i
	}

	newElements = &ElementsComposite{
		NArray:        arrays,
		Fields:         fields,
		FieldArraysMap: fieldArraysMap,
	}

	return
}

func (els ElementsComposite) IsNaN() (result []bool) {
	columnNum := els.numColumn()
	if columnNum == 0 {
		return
	}
	l := index.IndexBool(els.NArray[0].IsNaN())
	for i := 1; i < columnNum; i++ {
		l.And(els.NArray[i].IsNaN())
	}

	result = l
	return
}

func (els *ElementsComposite) Location(coord int) (element elements.ElementValue, err error) {
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	compositeLen := els.Len()
	if coord >= compositeLen {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, compositeLen))
		return
	}

	compositeResult := make(map[string]elements.ElementValue)
	columnNum := els.numColumn()
	for i := 0; i < columnNum; i++ {
		val, _ := els.NArray[i].At(coord)
		compositeResult[els.NArray[i].FieldName] = val
	}

	element.Value = compositeResult
	element.Type = types.TypeComposite
	return
}

func (els *ElementsComposite) Swap(i, j int) {
	columnNum := els.numColumn()
	for cI := 0; cI < columnNum; cI++ {
		els.NArray[cI].Swap(i, j)
	}
	return
}

func (els *ElementsComposite) checkAppendMapLikeRecords(values map[string]interface{}) (err error) {
	for key, value := range values {
		i, ok := els.FieldArraysMap[key]
		if !ok {
			err = errors.New(fmt.Sprintf("field %s is not in the composite", key))
			return
		}

		array := els.NArray[i]
		typ := array.Type()
		switch value.(type) {
		case []int, []int8, []int16, []int32, []int64:
			if typ != types.TypeInt {
				err = errors.New(fmt.Sprintf("can't append int value to %s array", typ))
				return
			}
		case []float32, []float64:
			if typ != types.TypeFloat {
				err = errors.New(fmt.Sprintf("can't append float value to %s array", typ))
				return
			}
		case []string:
			if typ != types.TypeString {
				err = errors.New(fmt.Sprintf("can't append string value to %s array", typ))
				return
			}
		case []bool:
			if typ != types.TypeBool {
				err = errors.New(fmt.Sprintf("can't append bool value to %s array", typ))
				return
			}
		case []interface{}:
			if typ != types.TypeObject {
				err = errors.New(fmt.Sprintf("can't append object value to %s array", typ))
				return
			}
		default:
			valueType := reflect.TypeOf(value).Kind().String()
			err = errors.New(fmt.Sprintf("type %s is not supported in this composite", valueType))
		}
	}

	return
}

func (els *ElementsComposite) appendMapLikeObject(values map[string]interface{}) {
	for key, value := range values {
		i := els.FieldArraysMap[key]
		array := els.NArray[i]

		switch value.(type) {
		case []int:
			val := value.([]int)
			for _, v := range val {
				array.Append(false, v)
			}
		case []int8:
			val := value.([]int8)
			for _, v := range val {
				array.Append(false, v)
			}
		case []int16:
			val := value.([]int16)
			for _, v := range val {
				array.Append(false, v)
			}
		case []int32:
			val := value.([]int32)
			for _, v := range val {
				array.Append(false, v)
			}
		case []float32:
			val := value.([]float32)
			for _, v := range val {
				array.Append(false, v)
			}
		case []float64:
			val := value.([]float64)
			for _, v := range val {
				array.Append(false, v)
			}
		case []string:
			val := value.([]string)
			for _, v := range val {
				array.Append(false, v)
			}
		case []bool:
			val := value.([]bool)
			for _, v := range val {
				array.Append(false, v)
			}
		case []interface{}:
			val := value.([]interface{})
			for _, v := range val {
				array.Append(false, v)
			}
		}
	}
	return
}

func (els *ElementsComposite) Append(copy bool, values ...interface{}) (newElements elements.Elements, err error) {
	if !copy {
		newElements = els
	} else {
		newElements = els.Copy()
	}
	nElements := newElements.(*ElementsComposite)

	for _, value := range values {
		switch value.(type) {
		case map[string]interface{}:
			val := value.(map[string]interface{})
			err = nElements.checkAppendMapLikeRecords(val)
			if err != nil {
				err = fmt.Errorf("append error: %w", err)
				return
			}
		default:
			typ := reflect.TypeOf(value).Kind().String()
			err = errors.New(fmt.Sprintf("append error: %s type is not supperted to append", typ))
			return
		}
	}

	for _, value := range values {
		switch value.(type) {
		case map[string]interface{}:
			val := value.(map[string]interface{})
			nElements.appendMapLikeObject(val)
		}
	}

	newElements = nElements
	return
}
