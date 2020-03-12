package io

import (
	"encoding/csv"
	"fmt"
	"io"
)

func ReadCSV(reader io.Reader) (dataMap map[string]interface{}, headers []string, err error) {
	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	records, e := csvReader.ReadAll()
	if e != nil {
		err = fmt.Errorf("read csv error: %w", e)
		return
	}

	dataMap = make(map[string]interface{})
	headers = records[0]
	rowNum := len(records)
	for columnI, header := range headers {
		dataColumn := make([]string, rowNum-1)
		for rowI := 1; rowI < rowNum; rowI++ {
			dataColumn[rowI-1] = records[rowI][columnI]
		}
		dataMap[header] = dataColumn
	}
	return
}