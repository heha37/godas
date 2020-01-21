package utils

import (
	"errors"
)

func GenerateSequenceInt(begin, end, step int) (sequence []int){
	if step == 0 {
		panic(errors.New("step must not be zero"))
	}
	count := 0
	if (end > begin && step > 0) || (end < begin && step < 0) {
		count = (end-step-begin)/step + 1
	}

	sequence = make([]int, count)
	for i := 0; i < count; i, begin = i+1, begin+step {
		sequence[i] = begin
	}
	return
}