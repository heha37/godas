package dataframe

import (
	"errors"
	"fmt"

	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/series"
	"github.com/hunknownz/godas/utils"
	"strconv"
)

type DataFrame struct {
	nSeries []*series.Series
	fields []string
	fieldSeriesMap map[string]int
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

func New(ses ...*series.Series) (df *DataFrame, err error) {
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
