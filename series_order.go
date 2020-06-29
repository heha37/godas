package godas

import (
	sfloat "github.com/hunknownz/godas/internal/elements_float"
	sstring "github.com/hunknownz/godas/internal/elements_string"
	"github.com/hunknownz/godas/types"
	"sort"
)

type seriesSorter struct {
	lessFunc  LessFunc
	ascending bool
	series    *Series
}

func (sorter *seriesSorter) Len() int {
	return sorter.series.Len()
}

func (sorter *seriesSorter) Less(i, j int) bool {
	return sorter.lessFunc(i, j)
}

func (sorter *seriesSorter) Swap(i, j int) {
	array := sorter.series.array
	array.Elements.Swap(i, j)
}

func (sorter *seriesSorter) Sort() {
	se := sorter.series
	if sorter.lessFunc == nil {
		typ := se.Type()
		switch typ {
		case types.TypeFloat:
			elements := se.array.Elements.(sfloat.ElementsFloat64)
			sortSlice := sort.Float64Slice(elements)
			if !sorter.ascending {
				sort.Sort(sort.Reverse(sortSlice))
			}
			sort.Sort(sortSlice)
		case types.TypeString:
			elements := se.array.Elements.(sstring.ElementsString)
			sortSlice := sort.StringSlice(elements)
			if !sorter.ascending {
				sort.Sort(sort.Reverse(sortSlice))
			}
			sort.Sort(sortSlice)
		}
		return
	}

	if !sorter.ascending {
		sort.Sort(sort.Reverse(sorter))
	} else {
		sort.Sort(sorter)
	}
}

func newSeriesSorter(se *Series, ascending bool, lessFunc LessFunc) (sorter *seriesSorter) {
	return &seriesSorter{
		lessFunc:  lessFunc,
		ascending: ascending,
		series:    se,
	}
}