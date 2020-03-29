package series_test

import (
	"fmt"
	"testing"
	"github.com/hunknownz/godas/series"
)

func TestNewSeries(t *testing.T) {
	dataInt := []int{
		1,2,3,4,5,
	}
	seriesInt := series.New(dataInt)
	fmt.Printf("%v\n", seriesInt)

	dataBool := []bool{
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
	}
	seriesBool := series.New(dataBool)
	fmt.Printf("%#v\n", seriesBool)

	dataString := []string{
		"test1", "test2", "NaN",
	}
	seriesString := series.New(dataString)
	fmt.Printf("%v\n", seriesString.IsNaN())
}

func TestSeriesCondition(t *testing.T) {
	dataInt := []int{
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,
	}
	seriesInt := series.New(dataInt)

	condition := series.NewCondition()
	condition.Or("<", 5)
	condition.And(">", 3)
	condition.Or(">", 7)
	condition.And("<", 9)
	condition.Or(">", 13)


	fmt.Printf("%v\n", condition)

	ixs, _ :=seriesInt.IsCondition(condition)
	fmt.Printf("%v\n", ixs)
}