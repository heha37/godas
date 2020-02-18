package dataframe

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/internal/condition"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/series"
	"github.com/hunknownz/godas/utils"
	"strconv"
)

type DataFrame struct {
	nSeries []*series.Series
	fields []string
	fieldSeriesMap map[string]int

	Err error
}

func (df *DataFrame) Len() int {
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
	if ses ==nil || len(ses) == 0 {
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
		key := strconv.Itoa(i)
		df.fields[i] = key
		df.fieldSeriesMap[key] = i
	}

	return
}
