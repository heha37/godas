package dataframe

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/order"
	"github.com/hunknownz/godas/types"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/hunknownz/godas/condition"
	"github.com/hunknownz/godas/index"
	gio "github.com/hunknownz/godas/internal/io"
	"github.com/hunknownz/godas/series"
	"github.com/hunknownz/godas/utils"
	"strconv"
)

type DataFrame struct {
	nSeries []*series.Series
	fields []string
	fieldSeriesMap map[string]int

	sourceType reflect.Type

	Err error
}

func (df *DataFrame) NumRow() int {
	if len(df.nSeries) != 0 {
		return df.nSeries[0].Len()
	}
	return 0
}

func (df *DataFrame) NumColumn() int {
	return len(df.fields)
}

func checkColumnsLengths(ses ...*series.Series) (rows, cols int, err error) {
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

func (df *DataFrame) checkSeriesLengths(ses ...*series.Series) (err error) {
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
	columnNum := df.NumColumn()
	ses := make([]*series.Series, columnNum)
	for i, se := range df.nSeries {
		newSeries, e := se.Subset(index)
		if e != nil {
			err = fmt.Errorf("sbuset dataframe error: %w", e)
			return
		}
		ses[i] = newSeries
	}

	_, colNum, err := checkColumnsLengths(ses...)
	if err != nil {
		err = fmt.Errorf("subset dataframe error: %w", err)
		return
	}

	fields := make([]string, colNum)
	fieldSeriesMap := make(map[string]int)
	for i, iField := range df.fields {
		fields[i] = iField
		fieldSeriesMap[iField] = i
	}

	newDataFrame = &DataFrame{
		nSeries:        ses,
		fields:         fields,
		fieldSeriesMap: fieldSeriesMap,
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
	iFields, iSeries, err := df.checkAndParseSelectionColumns(columns)
	if err != nil {
		err = fmt.Errorf("can't select columns: %w", err)
	}

	ses := make([]*series.Series, len(iSeries))
	for i, iS := range iSeries {
		ses[i] = df.nSeries[iS].Copy()
	}
	_, _, err = checkColumnsLengths(ses...)
	if err != nil {
		err = fmt.Errorf("new dataframe error: %w", err)
		return
	}

	fields := make([]string, len(iFields))
	fieldSeriesMap := make(map[string]int)
	for i, iField := range iFields {
		fields[i] = df.fields[iField]
		fieldSeriesMap[fields[i]] = i
	}

	newDataFrame = &DataFrame{
		nSeries:        ses,
		fields:         fields,
		fieldSeriesMap: fieldSeriesMap,
	}
	return
}

func (df *DataFrame) getOriginSeriesByColumn(column string) (se *series.Series, err error) {
	ok, _ := utils.ArrayContain(df.fields, column)
	if !ok {
		err = errors.New(fmt.Sprintf("column name %q not found", column))
		return
	}
	seI := df.fieldSeriesMap[column]
	se = df.nSeries[seI]
	return
}

func (df *DataFrame) GetSeriesByColumn(column string) (newSeries *series.Series, err error) {
	oldSeries, err := df.getOriginSeriesByColumn(column)
	newSeries = oldSeries.Copy()
	return
}

func (df *DataFrame) Copy() (newDataFrame *DataFrame) {
	newDataFrame, _ = NewFromSeries(df.nSeries...)
	return
}

// AssignSeries assigns new columns to a DataFrame by series.
func (df *DataFrame) AssignSeries(inplace bool, ses ...*series.Series) (newDataFrame *DataFrame, err error) {
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

	for _, se := range ses {
		newSe := se.Copy()
		newDataFrame.nSeries = append(newDataFrame.nSeries, newSe)
		i := len(newDataFrame.nSeries) - 1
		key := newSe.FieldName
		if key == "" {
			key = strconv.Itoa(i)
			newSe.FieldName = "C" + key
		}
		newDataFrame.fields = append(newDataFrame.fields, key)
		newDataFrame.fieldSeriesMap[key] = i
	}

	return
}

func (df *DataFrame) appendMapLikeObject(values map[string]interface{}) {
	for key, value := range values {
		i := df.fieldSeriesMap[key]
		se := df.nSeries[i]

		switch value.(type) {
		case []int:
			val := value.([]int)
			for _, v := range val {
				se.Append(false, v)
			}
		case []int8:
			val := value.([]int8)
			for _, v := range val {
				se.Append(false, v)
			}
		case []int16:
			val := value.([]int16)
			for _, v := range val {
				se.Append(false, v)
			}
		case []int32:
			val := value.([]int32)
			for _, v := range val {
				se.Append(false, v)
			}
		case []float32:
			val := value.([]float32)
			for _, v := range val {
				se.Append(false, v)
			}
		case []float64:
			val := value.([]float64)
			for _, v := range val {
				se.Append(false, v)
			}
		case []string:
			val := value.([]string)
			for _, v := range val {
				se.Append(false, v)
			}
		case []bool:
			val := value.([]bool)
			for _, v := range val {
				se.Append(false, v)
			}
		case []interface{}:
			val := value.([]interface{})
			for _, v := range val {
				se.Append(false, v)
			}
		}
	}
	return
}

func (df *DataFrame) checkAppendMapLikeRecords(values map[string]interface{}) (err error) {
	for key, value := range values {
		i, ok := df.fieldSeriesMap[key]
		if !ok {
			err = errors.New(fmt.Sprintf("field %s is not in the dataframe", key))
			return
		}

		se := df.nSeries[i]
		typ := se.Type()
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
		cmp := cond.CompItem
		seriesVal, err := df.GetSeriesByColumn(cmp.Column)
		if err != nil {
			df.Err = err
			return l
		}

		newCondition := series.NewCondition()
		newCondition.Or(cmp.Comparator, cmp.Value)
		ixs, err := seriesVal.IsCondition(newCondition)
		if err != nil {
			df.Err = err
			return ixs
		}
		return ixs
	}

	l = make(index.IndexBool, df.nSeries[0].Len())
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

func (df *DataFrame) Sort(inplace bool, sortKeys ...order.SortKey) (newDataFrame *DataFrame, err error) {
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
	for col := 0; col < colNum; col++ {
		df.nSeries[col].Swap(i, j)
	}
}

func (df *DataFrame) At(rowLabel int, columnLabel interface{}) (value elements.ElementValue, err error) {
	var i int
	switch columnLabel.(type) {
	case string:
		columnLabelString := columnLabel.(string)
		i = df.fieldSeriesMap[columnLabelString]
	case int:
		i = columnLabel.(int)
	default:
		typeString := reflect.TypeOf(columnLabel).Kind().String()
		err = fmt.Errorf("type %s is not supported", typeString)
	}

	se := df.nSeries[i]
	return se.At(rowLabel)
}

func (df *DataFrame) checkAndParseSelectionColumns(columns SelectionColumns) (iFields, iSeries []int, err error) {
	switch columns.(type) {
	case int:
		id := columns.(int)
		iFields = []int{id}
		key := df.fields[id]
		iSeries = []int{df.fieldSeriesMap[key]}
	case []int:
		iFields = columns.([]int)
		iSeries = make([]int, len(iFields))
		for i, id := range iFields {
			key := df.fields[id]
			iSeries[i] = df.fieldSeriesMap[key]
		}
	case string:
		key := columns.(string)
		ok, id := utils.ArrayContain(df.fields, key)
		if !ok {
			err = errors.New(fmt.Sprintf("column name %q not found", key))
			return
		}
		iFields = []int{id}
		iSeries = []int{df.fieldSeriesMap[key]}
	case []string:
		keys := columns.([]string)
		size := len(keys)
		iFields = make([]int, size)
		iSeries = make([]int, size)
		for i, key := range keys {
			ok, id := utils.ArrayContain(df.fields, key)
			if !ok {
				err = errors.New(fmt.Sprintf("can't select columns: column name %q", key))
				return
			}
			iFields[i] = id
			iSeries[i] = df.fieldSeriesMap[key]
		}
	default:
		err = errors.New("unknown selection columns")
		return
	}
	return
}

func NewCondition() *condition.Condition {
	return condition.NewCondition(condition.ConditionTypeDataFrame)
}

func NewFromSeries(ses ...*series.Series) (df *DataFrame, err error) {
	if ses == nil || len(ses) == 0 {
		df = &DataFrame{
			nSeries: make([]*series.Series, 0, 0),
			fields: make([]string, 0, 0),
			fieldSeriesMap: make(map[string]int),
		}
		return
	}

	_, colNum, err := checkColumnsLengths(ses...)
	if err != nil {
		err = fmt.Errorf("new dataframe error: %w", err)
		return
	}

	nSeries := make([]*series.Series, len(ses))
	for i, se := range ses {
		nSeries[i] = se.Copy()
	}

	df = &DataFrame{
		nSeries:   nSeries,
		fields: make([]string, colNum),
		fieldSeriesMap: make(map[string]int),
	}

	for i := 0; i < colNum; i++ {
		key := nSeries[i].FieldName
		if key == "" {
			key = strconv.Itoa(i)
			nSeries[i].FieldName = "C" + key
		}
		df.fields[i] = key
		df.fieldSeriesMap[key] = i
	}

	df.sourceType = generateAnonymousStructType(df)

	return
}

func generateAnonymousStructType(df *DataFrame) reflect.Type {
	columnNum := df.NumColumn()
	structFields := make([]reflect.StructField, columnNum)

	for i:=0; i < columnNum; i++ {
		se := df.nSeries[i]
		seType := se.Type()
		var structField reflect.StructField
		switch seType {
		case types.TypeInt:
			structField = reflect.StructField{
				Name:      se.FieldName,
				Type:      reflect.TypeOf(int64(0)),
			}
		case types.TypeBool:
			structField = reflect.StructField{
				Name:      se.FieldName,
				Type:      reflect.TypeOf(true),
			}
		case types.TypeFloat:
			structField = reflect.StructField{
				Name:      se.FieldName,
				Type:      reflect.TypeOf(float64(0)),
			}
		case types.TypeString:
			structField = reflect.StructField{
				Name:      se.FieldName,
				Type:      reflect.TypeOf(""),
			}
		}
		structFields[i] = structField
	}
	return reflect.StructOf(structFields)
}

func generateTypeSeries(valuesValue reflect.Value, fieldIndex int, valueType string, fieldName string, ptrFlag bool)(newSeries *series.Series) {
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
		newSeries, _ = series.New(elements, fieldName)
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
		newSeries, _ = series.New(elements, fieldName)
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
		newSeries, _ = series.New(elements, fieldName)
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
		newSeries, _ = series.New(elements, fieldName)
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
		newSeries, _ = series.New(elements, fieldName)
	}
	return
}

func NewFromStructs(values interface{}) (df *DataFrame, err error) {
	if values == nil {
		df = &DataFrame{
			nSeries: make([]*series.Series, 0, 0),
			fields: make([]string, 0, 0),
			fieldSeriesMap: make(map[string]int),
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
			nSeries: make([]*series.Series, 0, 0),
			fields: make([]string, 0, 0),
			fieldSeriesMap: make(map[string]int),
		}
		return
	}

	fieldsNum := valueType.NumField()
	nSeries := make([]*series.Series, fieldsNum)

	for i := 0; i < fieldsNum; i++ {
		field := valueType.Field(i)
		fieldName := field.Name
		fieldType := field.Type.String()

		nSeries[i] = generateTypeSeries(valuesValue, i, fieldType, fieldName, ptrFlag)
	}

	df, err = NewFromSeries(nSeries...)
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
	for i := 0; i < columnNum; i++ {
		se := df.nSeries[i]
		lValue := val.FieldByName(se.FieldName)
		elem, e := se.At(rowLabel)
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
	df = &DataFrame{
		nSeries: make([]*series.Series, colNum),
		fields: headers,
		fieldSeriesMap: make(map[string]int),
	}
	for columnI, header := range headers {
		df.fieldSeriesMap[header] = columnI
		df.nSeries[columnI], _ = series.New(dataMap[header], header)
	}

	df.sourceType = generateAnonymousStructType(df)

	return
}