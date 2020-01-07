package dataframe

import (
	"errors"
	"fmt"

	"github.com/heha37/godas/series"
)

type DataFrame struct {
	nSeries []*series.Series
	fields []interface{}
	fieldsMap map[interface{}]int
	rows int
	cols int
}

func New(ses ...*series.Series) (df *DataFrame, err error) {
	if ses ==nil || len(ses) == 0 {
		df = &DataFrame{
			nSeries: make([]*series.Series, 0, 0),
			fields: make([]interface{}, 0, 0),
			fieldsMap: make(map[interface{}]int),
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
		fieldsMap: make(map[interface{}]int),
		rows:      rows,
		cols:      cols,
	}

	for i := 0; i < cols; i++ {
		df.fields[i] = i
		df.fieldsMap[i] = i
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