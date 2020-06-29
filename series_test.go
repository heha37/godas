package series_test

import (
	"fmt"
	"github.com/hunknownz/godas/order"
	"testing"
	"github.com/hunknownz/godas/series"
)

func TestNewSeries(t *testing.T) {
	dataInt := []int{
		1,2,3,4,5,
	}
	seriesInt, _ := series.New(dataInt, "test")
	valInt, _ := seriesInt.At(2)
	fmt.Printf("%v\n", valInt.MustInt())

	dataBool := []bool{
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
		true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,
	}
	seriesBool, _ := series.New(dataBool, "test")
	boolValue, _ := seriesBool.At(2)
	fmt.Printf("%v\n", boolValue.MustBool())

	dataString := []string{
		"test1", "test2", "NaN",
	}
	seriesString, _ := series.New(dataString, "text")
	fmt.Printf("%v\n", seriesString.IsNaN())
}

func TestSeriesCondition(t *testing.T) {
	dataInt := []int{
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,
	}
	seriesInt, _ := series.New(dataInt, "test")

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

func TestSeriesSort(t *testing.T) {
	dataInt := []int{
		5,4,1,3,8,6,9,2,1,5,
	}
	seriesInt, _ := series.New(dataInt, "test")

	f := func(a, b int64) bool {
		return a < b
	}
	lessFunc := seriesInt.NewIntLessFunc(order.IntLessFunc(f))
	seriesInt.Sort(true, true, lessFunc)
	for i:=0; i<seriesInt.Len(); i++ {
		val, _ := seriesInt.At(i)
		fmt.Printf("%v\n", val.MustInt())
	}
}