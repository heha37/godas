package dataframe

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/internal/elements"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/condition"
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

func (df *DataFrame) Len() int {
	if len(df.nSeries) != 0 {
		return df.nSeries[0].Len()
	}
	return 0
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
			err = errors.New("elements_int must all be same length")
			return
		}
	}
	return
}

func (df *DataFrame) Subset(index index.IndexInt) (newDataFrame *DataFrame, err error) {
	rowNum := df.Len()
	ses := make([]*series.Series, rowNum)
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

func (df *DataFrame) GetSeriesByColumn(column string) (newSeries *series.Series, err error) {
	ok, _ := utils.ArrayContain(df.fields, column)
	if !ok {
		err = errors.New(fmt.Sprintf("column name %q not found", column))
		return
	}
	oldSeriesI := df.fieldSeriesMap[column]
	oldSeries := df.nSeries[oldSeriesI]
	newSeries = oldSeries.Copy()
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

func (df *DataFrame) At(rowLabel int, columnLabel string) (value elements.ElementValue, err error) {
	i := df.fieldSeriesMap[columnLabel]
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
		}
		df.fields[i] = key
		df.fieldSeriesMap[key] = i
	}

	return
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
		newSeries = series.New(elements, fieldName)
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
		newSeries = series.New(elements, fieldName)
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
		newSeries = series.New(elements, fieldName)
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
		newSeries = series.New(elements, fieldName)
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
		df.nSeries[columnI] = series.New(dataMap[header], header)
	}
	return
}