package elements_bool

import (
	"github.com/hunknownz/godas/internal"
)

func NewElementsBool(elements []bool) (newElements ElementsBool) {
	boolSliceLen := len(elements)
	newBitsSliceLen := boolSliceLen >> 4
	if (newBitsSliceLen << 4) != boolSliceLen {
		newBitsSliceLen = newBitsSliceLen + 1
	}
	newBits := make([]uint32, newBitsSliceLen)
	newBitBools := BitBools{
		bits: newBits,
		bitsSliceLen: uint32(newBitsSliceLen),
	}
	newBitBools.clearBits()

	for bitsI, value := range elements {
		boolValue := internal.If(value == true, trueValue, falseValue)
		newBitBools.set(bitsI, boolValue.(bitBoolValue))
	}

	newElements = newBitBools
	return
}