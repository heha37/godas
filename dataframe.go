package godas

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/internal"
	"github.com/hunknownz/godas/internal/elements"
	sbool "github.com/hunknownz/godas/internal/elements_bool"
	ec "github.com/hunknownz/godas/internal/elements_composite"
	"github.com/hunknownz/godas/types"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/hunknownz/godas/condition"
	"github.com/hunknownz/godas/index"
	gio "github.com/hunknownz/godas/internal/io"
	sfloat "github.com/hunknownz/godas/internal/elements_float"
	sint "github.com/hunknownz/godas/internal/elements_int"
	sobject "github.com/hunknownz/godas/internal/elements_object"
	sstring "github.com/hunknownz/godas/internal/elements_string"
	"strconv"
)

type DataFrame struct {
	data *ec.ElementsComposite

	sourceType reflect.Type
	Err error
}

func (df *DataFrame) NumRow() int {
	if len(df.data.NArray) != 0 {
		return df.data.NArray[0].Len()
	}
	return 0
}

func (df *DataFrame) NumColumn() int {
	return len(df.data.Fields)
}

func checkArraysColumnsLengths(arrays ...*ec.Array) (rows, cols int, err error) {
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

func checkSeriesColumnsLengths(ses ...*Series) (rows, cols int, err error) {
	cols = len(ses)
	rows = -1
	if ses == nil || cols == 0 {
		return
	}

	rows = ses[0].Len()
	for i:=1; i<cols; i++ {
		if rows != ses[i].Len() {
			err = errors.New("elements must all be same length")
			return
		}
	}
	return
}

func (df *DataFrame) checkSeriesLengths(ses ...*Series) (err error) {
	cols := len(ses)
	rows := df.NumRow()
	for i:=0; i<cols; i++ {
		if rows != ses[i].Len() {
			err = errors.New("series must all be same length with dataframe")
			return
		}
	}

	return
}

func (df *DataFrame) Subset(index index.IndexInt) (newDataFrame *DataFrame, err error) {
	newElements, err := df.data.Subset(index)
	if err != nil {
		err = fmt.Errorf("subset dataframe error: %w", err)
		return
	}

	newDataFrame = &DataFrame{
		data:       newElements.(*ec.ElementsComposite),
		sourceType: df.sourceType,
		Err:        nil,
	}
	return
}

// SelectionColumns support indexes are:
//    int
//    []int
//    string
//    []string
type SelectionColumns interface{}

func (df *DataFrame) Select(columns SelectionColumns) (newDataFrame *DataFrame, err error) {
	data := df.data
	iFields, iSeries, err := df.checkAndParseSelectionColumns(columns)
	if err != nil {
		err = fmt.Errorf("can't select columns: %w", err)
	}

	arrays := make([]*ec.Array, len(iSeries))
	for i, iS := range iSeries {
		arrays[i] = data.NArray[iS].Copy()
	}
	_, _, err = checkArraysColumnsLengths(arrays...)
	if err != nil {
		err = fmt.Errorf("new dataframe error: %w", err)
		return
	}

	fields := make([]string, len(iFields))
	fieldArraysMap := make(map[string]int)
	for i, iField := range iFields {
		fields[i] = data.Fields[iField]
		fieldArraysMap[fields[i]] = i
	}

	newData := &ec.ElementsComposite{
		NArray:         arrays,
		Fields:         fields,
		FieldArraysMap: fieldArraysMap,
	}
	newDataFrame = &DataFrame{
		data:        newData,
	}
	newDataFrame.sourceType = generateAnonymousStructType(newDataFrame)
	return
}

func (df *DataFrame) getOriginSeriesByColumn(column string) (se *Series, err error) {
	data := df.data
	ok, _ := internal.ArrayContain(data.Fields, column)
	if !ok {
		err = errors.New(fmt.Sprintf("column name %q not found", column))
		return
	}
	arrayI := data.FieldArraysMap[column]
	array := data.NArray[arrayI]

	se = &Series{
		array: array.Copy(),
	}
	return
}

func (df *DataFrame) GetSeriesByColumn(column string) (newSeries *Series, err error) {
	newSeries, err = df.getOriginSeriesByColumn(column)
	if err != nil {
		err = fmt.Errorf("get series by column error: %w", err)
		return
	}
	return
}

func (df *DataFrame) Copy() (newDataFrame *DataFrame) {
	data := df.data
	newDataFrame, _ = newFromArrays(data.NArray...)
	return
}

// AssignSeries assigns new columns to a DataFrame by series.
func (df *DataFrame) AssignSeries(inplace bool, ses ...*Series) (newDataFrame *DataFrame, err error) {
	err = df.checkSeriesLengths(ses...)
	if err != nil {
		err = fmt.Errorf("assign dataframe error: %w", err)
		return
	}

	if inplace {
		newDataFrame = df
	} else {
		newDataFrame = df.Copy()
	}

	data := newDataFrame.data
	for _, se := range ses {
		newArray := se.array.Copy()
		data.NArray = append(data.NArray, newArray)
		i := len(data.NArray) - 1
		key := newArray.FieldName
		if key == "" {
			key = strconv.Itoa(i)
			newArray.FieldName = "C" + key
		}
		data.Fields = append(data.Fields, key)
		data.FieldArraysMap[key] = i
	}
	newDataFrame.sourceType = generateAnonymousStructType(newDataFrame)

	return
}

func (df *DataFrame) appendMapLikeObject(values map[string]interface{}) {
	data := df.data
	for key, value := range values {
		i := data.FieldArraysMap[key]
		array := data.NArray[i]

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

func (df *DataFrame) checkAppendMapLikeRecords(values map[string]interface{}) (err error) {
	data := df.data
	for key, value := range values {
		i, ok := data.FieldArraysMap[key]
		if !ok {
			err = errors.New(fmt.Sprintf("field %s is not in the dataframe", key))
			return
		}

		array := data.NArray[i]
		typ := array.Type()
		switch value.(type) {
		case []int, []int8, []int16, []int32, []int64:
			if typ != types.TypeInt {
				err = errors.New(fmt.Sprintf("can't append int value to %s series", typ))
				return
			}
		case []float32, []float64:
			if typ != types.TypeFloat {
				err = errors.New(fmt.Sprintf("can't append float value to %s series", typ))
				return
			}
		case []string:
			if typ != types.TypeString {
				err = errors.New(fmt.Sprintf("can't append string value to %s series", typ))
				return
			}
		case []bool:
			if typ != types.TypeBool {
				err = errors.New(fmt.Sprintf("can't append bool value to %s series", typ))
				return
			}
		case []interface{}:
			if typ != types.TypeObject {
				err = errors.New(fmt.Sprintf("can't append object value to %s series", typ))
				return
			}
		default:
			valueType := reflect.TypeOf(value).Kind().String()
			err = errors.New(fmt.Sprintf("type %s is not supported in this dataframe", valueType))
		}
	}

	return
}

func (df *DataFrame) Append(copy bool, records ...interface{}) (newDataFrame *DataFrame, err error) {
	if !copy {
		newDataFrame = df
	} else {
		newDataFrame = df.Copy()
	}

	for _, record := range records {
		switch record.(type) {
		case map[string]interface{}:
			val := record.(map[string]interface{})
			err = newDataFrame.checkAppendMapLikeRecords(val)
			if err != nil {
				err = fmt.Errorf("append error: %w", err)
				return
			}
		default:
			typ := reflect.TypeOf(record).Kind().String()
			err = errors.New(fmt.Sprintf("append error: %s type is not supperted to append", typ))
			return
		}
	}

	for _, record := range records {
		switch record.(type) {
		case map[string]interface{}:
			val := record.(map[string]interface{})
			newDataFrame.appendMapLikeObject(val)
		}
	}

	return
}

func (df *DataFrame) evaluateCondition(expr condition.ExprAST) index.IndexBool {
	var l, r index.IndexBool
	switch expr.(type) {
	case condition.BinaryExprAST:
		ast := expr.(condition.BinaryExprAST)
		l = df.evaluateCondition(ast.Lhs)
		r = df.evaluateCondition(ast.Rhs)
		switch ast.Op {
		case "&&":
			l.And(r)
			return l
		case "||":
			l.Or(r)
			return l
		}
	case condition.ValueExprAST:
		cond := expr.(condition.ValueExprAST).Value
		if cond.Cond != nil {
			nextExpr := cond.Cond.Prepare()
			return df.evaluateCondition(nextExpr)
		}
		cmp := cond.CompItem
		seriesVal, err := df.GetSeriesByColumn(cmp.Column)
		if err != nil {
			df.Err = err
			return l
		}

		newCondition := NewDataFrameCondition()
		newCondition.Or(cmp.Comparator, cmp.Value)
		ixs, err := seriesVal.IsCondition(newCondition)
		if err != nil {
			df.Err = err
			return ixs
		}
		return ixs
	}

	data := df.data
	l = make(index.IndexBool, data.NArray[0].Len())
	for i := 0; i < len(l); i++ {
		l[i] = true
	}
	return l
}

func (df *DataFrame) IsCondition(cond *condition.Condition) (ixs index.IndexBool, err error) {
	expr := cond.Prepare()
	ixs = df.evaluateCondition(expr)
	if df.Err != nil {
		err = fmt.Errorf("filter error: %w", df.Err)
		return
	}
	return
}

func (df *DataFrame) Filter(cond *condition.Condition) (newDataFrame *DataFrame, err error) {
	ixs, err := df.IsCondition(cond)
	if err != nil {
		err = fmt.Errorf("filter error: %w", err)
		return
	}
	idx := make(index.IndexInt, 0)
	for ix, ixVal := range ixs {
		if ixVal {
			idx = append(idx, uint32(ix))
		}
	}
	newDataFrame, err = df.Subset(idx)
	if err != nil {
		err = fmt.Errorf("filter error: %w", err)
		return
	}
	return
}

func (df *DataFrame) Sort(inplace bool, sortKeys ...SortKey) (newDataFrame *DataFrame, err error) {
	if inplace {
		newDataFrame = df
	} else {
		newDataFrame = df.Copy()
	}

	sorter, err := newDataframeSorter(df, sortKeys...)
	if err != nil {
		err = fmt.Errorf("sort error: %w", err)
	}
	sorter.Sort()
	return
}

func (df *DataFrame) Swap(i, j int) {
	colNum := df.NumColumn()
	data := df.data
	for col := 0; col < colNum; col++ {
		data.NArray[col].Swap(i, j)
	}
}

func (df *DataFrame) At(rowLabel int, columnLabel interface{}) (value elements.ElementValue, err error) {
	var i int
	data := df.data
	switch columnLabel.(type) {
	case string:
		columnLabelString := columnLabel.(string)
		i = data.FieldArraysMap[columnLabelString]
	case int:
		i = columnLabel.(int)
	default:
		typeString := reflect.TypeOf(columnLabel).Kind().String()
		err = fmt.Errorf("type %s is not supported", typeString)
	}

	array := data.NArray[i]
	return array.At(rowLabel)
}

func (df *DataFrame) checkAndParseSelectionColumns(columns SelectionColumns) (iFields, iSeries []int, err error) {
	data := df.data
	switch columns.(type) {
	case int:
		id := columns.(int)
		iFields = []int{id}
		key := data.Fields[id]
		iSeries = []int{data.FieldArraysMap[key]}
	case []int:
		iFields = columns.([]int)
		iSeries = make([]int, len(iFields))
		for i, id := range iFields {
			key := data.Fields[id]
			iSeries[i] = data.FieldArraysMap[key]
		}
	case string:
		key := columns.(string)
		ok, id := internal.ArrayContain(data.Fields, key)
		if !ok {
			err = errors.New(fmt.Sprintf("column name %q not found", key))
			return
		}
		iFields = []int{id}
		iSeries = []int{data.FieldArraysMap[key]}
	case []string:
		keys := columns.([]string)
		size := len(keys)
		iFields = make([]int, size)
		iSeries = make([]int, size)
		for i, key := range keys {
			ok, id := internal.ArrayContain(data.Fields, key)
			if !ok {
				err = errors.New(fmt.Sprintf("can't select columns: column name %q", key))
				return
			}
			iFields[i] = id
			iSeries[i] = data.FieldArraysMap[key]
		}
	default:
		err = errors.New("unknown selection columns")
		return
	}
	return
}

func NewDataFrameCondition() *condition.Condition {
	return condition.NewCondition(condition.ConditionTypeDataFrame)
}

func newFromArrays(arrays ...*ec.Array) (df *DataFrame, err error) {
	if arrays == nil || len(arrays) == 0 {
		df = &DataFrame{
			data: newEmptyData(),
		}
		return
	}

	_, colNum, err := checkArraysColumnsLengths(arrays...)
	if err != nil {
		err = fmt.Errorf("new dataframe error: %w", err)
		return
	}

	nArray := make([]*ec.Array, len(arrays))
	for i, array := range arrays {
		nArray[i] = array.Copy()
	}

	df = &DataFrame{
		data: &ec.ElementsComposite{
			NArray:         nArray,
			Fields:         make([]string, colNum),
			FieldArraysMap: make(map[string]int),
		},
	}

	for i := 0; i < colNum; i++ {
		key := nArray[i].FieldName
		if key == "" {
			key = "C" + strconv.Itoa(i)
			nArray[i].FieldName = key
		}
		df.data.Fields[i] = key
		df.data.FieldArraysMap[key] = i
	}

	df.sourceType = generateAnonymousStructType(df)

	return
}

func NewFromSeries(ses ...*Series) (df *DataFrame, err error) {
	if ses == nil || len(ses) == 0 {
		df = &DataFrame{
			data: newEmptyData(),
		}
		return
	}

	_, colNum, err := checkSeriesColumnsLengths(ses...)
	if err != nil {
		err = fmt.Errorf("new dataframe error: %w", err)
		return
	}

	nArray := make([]*ec.Array, len(ses))
	for i, se := range ses {
		nArray[i] = se.array.Copy()
	}

	newData := &ec.ElementsComposite{
		NArray:         nArray,
		Fields:         make([]string, colNum),
		FieldArraysMap: make(map[string]int),
	}
	df = &DataFrame{
		data: newData,
	}

	for i := 0; i < colNum; i++ {
		key := nArray[i].FieldName
		if key == "" {
			key = "C" + strconv.Itoa(i)
			nArray[i].FieldName = key
		}
		newData.Fields[i] = key
		newData.FieldArraysMap[key] = i
	}

	df.sourceType = generateAnonymousStructType(df)

	return
}

func generateAnonymousStructType(df *DataFrame) reflect.Type {
	columnNum := df.NumColumn()
	structFields := make([]reflect.StructField, columnNum)

	data := df.data
	for i:=0; i < columnNum; i++ {
		array := data.NArray[i]
		seType := array.Type()
		var structField reflect.StructField
		switch seType {
		case types.TypeInt:
			structField = reflect.StructField{
				Name:      array.FieldName,
				Type:      reflect.TypeOf(int64(0)),
			}
		case types.TypeBool:
			structField = reflect.StructField{
				Name:      array.FieldName,
				Type:      reflect.TypeOf(true),
			}
		case types.TypeFloat:
			structField = reflect.StructField{
				Name:      array.FieldName,
				Type:      reflect.TypeOf(float64(0)),
			}
		case types.TypeString:
			structField = reflect.StructField{
				Name:      array.FieldName,
				Type:      reflect.TypeOf(""),
			}
		}
		structFields[i] = structField
	}
	return reflect.StructOf(structFields)
}

func generateTypeArrays(valuesValue reflect.Value, fieldIndex int, valueType string, fieldName string, ptrFlag bool) (newArray *ec.Array) {
	seriesLen := valuesValue.Len()

	switch valueType {
	case "float", "float32", "float64":
		elements := make([]float64, seriesLen)
		for i := 0; i < seriesLen; i++ {
			var val reflect.Value
			if ptrFlag {
				val = valuesValue.Index(i).Elem().Field(fieldIndex)
			} else {
				val = valuesValue.Index(i).Field(fieldIndex)
			}

			elements[i] = val.Float()
		}
		newElements := sfloat.NewElementsFloat64(elements)
		newArray = &ec.Array{
			FieldName: fieldName,
			Elements:  newElements,
		}
	case "int8", "int16", "int", "int32", "int64":
		elements := make([]int64, seriesLen)
		for i := 0; i < seriesLen; i++ {
			var val reflect.Value
			if ptrFlag {
				val = valuesValue.Index(i).Elem().Field(fieldIndex)
			} else {
				val = valuesValue.Index(i).Field(fieldIndex)
			}

			elements[i] = val.Int()
		}
		newElements := sint.NewElementsInt64(elements)
		newArray = &ec.Array{
			FieldName: fieldName,
			Elements:  newElements,
		}
	case "string":
		elements := make([]string, seriesLen)
		for i := 0; i < seriesLen; i++ {
			var val reflect.Value
			if ptrFlag {
				val = valuesValue.Index(i).Elem().Field(fieldIndex)
			} else {
				val = valuesValue.Index(i).Field(fieldIndex)
			}

			elements[i] = val.String()
		}
		newElements := sstring.NewElementsString(elements)
		newArray = &ec.Array{
			FieldName: fieldName,
			Elements:  newElements,
		}
	case "bool":
		elements := make([]bool, seriesLen)
		for i := 0; i < seriesLen; i++ {
			var val reflect.Value
			if ptrFlag {
				val = valuesValue.Index(i).Elem().Field(fieldIndex)
			} else {
				val = valuesValue.Index(i).Field(fieldIndex)
			}

			elements[i] = val.Bool()
		}
		newElements := sbool.NewElementsBool(elements)
		newArray = &ec.Array{
			FieldName: fieldName,
			Elements:  newElements,
		}
	default:
		elements := make([]interface{}, seriesLen)
		for i := 0; i < seriesLen; i++ {
			var val reflect.Value
			if ptrFlag {
				val = valuesValue.Index(i).Elem().Field(fieldIndex)
			} else {
				val = valuesValue.Index(i).Field(fieldIndex)
			}

			elements[i] = val.Interface()
		}
		newElements := sobject.NewElementsObject(elements)
		newArray = &ec.Array{
			FieldName: fieldName,
			Elements:  newElements,
		}
	}
	return
}

func newEmptyData() *ec.ElementsComposite {
	return  &ec.ElementsComposite{
		NArray:         make([]*ec.Array, 0, 0),
		Fields:         make([]string, 0, 0),
		FieldArraysMap: make(map[string]int),
	}
}

func NewFromStructs(values interface{}) (df *DataFrame, err error) {
	if values == nil {
		df = &DataFrame{
		    data: newEmptyData(),
		}
		return
	}

	valuesType, valuesValue := reflect.TypeOf(values), reflect.ValueOf(values)
	valuesKind := valuesType.Kind()
	if valuesKind != reflect.Slice {
		err = fmt.Errorf("type %s isn't supported, must be slice", valuesKind)
		return
	}

	ptrFlag := false
	valueType := valuesType.Elem()
	valueKind := valueType.Kind()
	if valueKind != reflect.Ptr && valueKind != reflect.Struct {
		err = fmt.Errorf("type %s isn't supported, must be struct slice", valueKind)
		return
	}
	if valueKind == reflect.Ptr {
		valueType = valueType.Elem()
		valueKind = valueType.Kind()
		ptrFlag = true
	}
	if valueKind != reflect.Struct {
		err = fmt.Errorf("type %s isn't supported, must be struct slice", valueKind)
		return
	}

	if valuesValue.Len() == 0 {
		df = &DataFrame{
			data: newEmptyData(),
		}
		return
	}

	fieldsNum := valueType.NumField()
	nArray := make([]*ec.Array, fieldsNum)

	for i := 0; i < fieldsNum; i++ {
		field := valueType.Field(i)
		fieldName := field.Name
		fieldType := field.Type.String()

		nArray[i] = generateTypeArrays(valuesValue, i, fieldType, fieldName, ptrFlag)
	}

	df, err = newFromArrays(nArray...)
	if err != nil {
		err = fmt.Errorf("new dataframe from series error: %w", err)
		return
	}

	df.sourceType = valueType
	return
}

func (df *DataFrame) IndexStruct(rowLabel int) (rowStruct interface{}, err error) {
	val := reflect.New(df.sourceType).Elem()
	columnNum := df.NumColumn()

	data := df.data
	for i := 0; i < columnNum; i++ {
		array := data.NArray[i]
		lValue := val.FieldByName(array.FieldName)
		elem, e := array.At(rowLabel)
		if e != nil {
			err = fmt.Errorf("series at %d error: %w", rowLabel, e)
			return
		}
		switch lValue.Type().Kind() {
		case reflect.String:
			rValue := elem.MustString()
			lValue.SetString(rValue)
		case reflect.Bool:
			rValue := elem.MustBool()
			lValue.SetBool(rValue)
		case reflect.Int:
			rValue := elem.MustInt()
			lValue.SetInt(rValue)
		case reflect.Int64:
			rValue := elem.MustInt()
			lValue.SetInt(rValue)
		case reflect.Float64:
			rValue := elem.MustFloat()
			lValue.SetFloat(rValue)
		default:
			rValue := elem.MustInterface()
			v := reflect.ValueOf(rValue)
			lValue.Set(v)
		}
	}
	rowStruct = val.Addr().Interface()
	return
}

func (df *DataFrame) ToStructs() (structs []interface{}) {
	rowNum := df.NumRow()
	for i := 0; i < rowNum; i++ {
		rowStruct, _ := df.IndexStruct(i)
		structs = append(structs, rowStruct)
	}
	return
}

func NewFromCSV(filepathOrBufferstr interface{}) (df *DataFrame, err error) {
	var	reader io.Reader
	switch filepathOrBufferstr.(type) {
	case string:
		filepath := filepathOrBufferstr.(string)
		b, e := ioutil.ReadFile(filepath)
		if e != nil {
			err = fmt.Errorf("read file %s error: %w", filepath, e)
			return
		}
		reader = strings.NewReader(string(b))
	}
	dataMap, headers, err := gio.NewFromCSV(reader)
	if err != nil {
		err = fmt.Errorf("read csv error: %w", err)
		return
	}
	
	colNum := len(headers)
	newData := &ec.ElementsComposite{
		NArray:         make([]*ec.Array, colNum),
		Fields:         headers,
		FieldArraysMap: make(map[string]int),
	}
	df = &DataFrame{
		data: newData,
	}
	for columnI, header := range headers {
		newData.FieldArraysMap[header] = columnI
		newData.NArray[columnI], _ = ec.NewArray(dataMap[header], header)
	}

	df.sourceType = generateAnonymousStructType(df)

	return
}