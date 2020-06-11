package dataframe

import (
	"fmt"
	"github.com/hunknownz/godas/order"
	"github.com/hunknownz/godas/types"
	"math/big"
	"sort"
	"strings"
)

type dataframeSorter struct {
	sortKeys []order.SortKey
	dataframe *DataFrame
}

func (sorter *dataframeSorter) Len() int {
	return sorter.dataframe.NumRow()
}

func (sorter *dataframeSorter) Swap(i, j int) {
	sorter.dataframe.Swap(i, j)
}

func (sorter *dataframeSorter) Less(i, j int) bool {
	var k int
	for k = 0; k < len(sorter.sortKeys)-1; k++ {
		less := sorter.sortKeys[k].LessFunc
		ascending := sorter.sortKeys[k].Ascending
		switch {
		case less(i, j):
			return ascending
		case less(j, i):
			return !ascending
		}
	}

	ascending := sorter.sortKeys[k].Ascending
	switch {
	case sorter.sortKeys[k].LessFunc(i, j):
		return ascending
	default:
		return !ascending
	}
}

func (sorter *dataframeSorter) Sort() {
	sort.Sort(sorter)
	return
}

func newDataframeSorter(df *DataFrame, sortKeys ...order.SortKey) (sorter *dataframeSorter, err error) {
	for i := range sortKeys {
		if sortKeys[i].LessFunc == nil {
			se, e := df.getOriginSeriesByColumn(sortKeys[i].Column)
			if e != nil {
				err = fmt.Errorf("sort error: %w", e)
				return
			}

			seType := se.Type()
			switch seType {
			case types.TypeInt:
				f := func(a, b int64) bool {
					return a < b
				}
				sortKeys[i].LessFunc = se.NewIntLessFunc(f)
			case types.TypeString:
				f := func(a, b string) bool {
					return strings.Compare(a, b) < 0
				}
				sortKeys[i].LessFunc = se.NewStringLessFunc(f)
			case types.TypeFloat:
				f := func(a, b float64) bool {
					return big.NewFloat(a).Cmp(big.NewFloat(b)) < 0
				}
				sortKeys[i].LessFunc = se.NewFloatLessFunc(f)
			case types.TypeBool:
				f := func(a, b bool) bool {
					return !a && b
				}
				sortKeys[i].LessFunc = se.NewBoolLessFunc(f)
			}
		}
	}

	sorter = &dataframeSorter{
		sortKeys:  sortKeys,
		dataframe: df,
	}
	return
}
