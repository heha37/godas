package dataframe

import (
	"errors"
	"fmt"
	"github.com/heha37/godas/utils"

	"github.com/heha37/godas/series"
)

type DataFrame struct {
	nSeries []*series.Series
	fields []interface{}
	fieldSeriesMap map[interface{}]int
	rows int
	cols int
}

func New(ses ...*series.Series) (df *DataFrame, err error) {
	if ses ==nil || len(ses) == 0 {
		df = &DataFrame{
			nSeries: make([]*series.Series, 0, 0),
			fields: make([]interface{}, 0, 0),
			fieldSeriesMap: make(map[interface{}]int),
			rows: 0,
			cols: 0,
		}
		return
	}

	rows, cols, err := checkColumnsLengths(ses...)
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
		fields: make([]interface{}, cols),
		fieldSeriesMap: make(map[interface{}]int),
		rows:      rows,
		cols:      cols,
	}

	for i := 0; i < cols; i++ {
		df.fields[i] = i
		df.fieldSeriesMap[i] = i
	}

	return
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
			err = errors.New("series must all be same length")
			return
		}
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
	iFields, iSeries, err := df.checkAndParseSelectionIndex(columns)
	if err != nil {
		err = fmt.Errorf("can't select columns: %w", err)
	}

	ses := make([]*series.Series, len(iSeries))
	for i, iS := range iSeries {
		ses[i] = df.nSeries[iS].Copy()
	}
	rows, cols, err := checkColumnsLengths(ses...)
	if err != nil {
		err = fmt.Errorf("new dataframe error: %w", err)
		return
	}

	fields := make([]interface{}, len(iFields))
	fieldSeriesMap := make(map[interface{}]int)
	for i, iField := range iFields {
		fields[i] = df.fields[iField]
		fieldSeriesMap[fields[i]] = i
	}

	newDataFrame = &DataFrame{
		nSeries:        ses,
		fields:         fields,
		fieldSeriesMap: fieldSeriesMap,
		rows:           rows,
		cols:           cols,
	}
	return
}

func (df *DataFrame) checkAndParseSelectionIndex(index SelectionColumns) (iFields, iSeries []int, err error) {
	switch index.(type) {
	case int:
		id := index.(int)
		iFields = []int{id}
		key := df.fields[id]
		iSeries = []int{df.fieldSeriesMap[key]}
	case []int:
		iFields = index.([]int)
		iSeries = make([]int, len(iFields))
		for i, id := range iFields {
			key := df.fields[id]
			iSeries[i] = df.fieldSeriesMap[key]
		}
	case string:
		key := index.(string)
		ok, id := utils.ArrayContain(df.fields, key)
		if !ok {
			err = errors.New(fmt.Sprintf("column name %q not found", key))
			return
		}
		iFields = []int{id}
		iSeries = []int{df.fieldSeriesMap[key]}
	case []string:
		keys := index.([]string)
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
