package elements_bool

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/utils"

	"github.com/hunknownz/godas/types"
)

type Elements = BitBools

func (elements Elements) Type() (sType types.ElementsType) {
	return types.ElementsTypeBool
}

func (elements Elements) Len() int {
	return elements.bitSliceLen()
}

func (elements Elements) String() string {
	return fmt.Sprint(elements)
}

func (elements Elements) Copy() (newElements elements.Elements) {
	newBits := make([]uint32, elements.bitsSliceLen)
	copy(newBits, elements.bits)
	newBitBools := BitBools{
		bits: newBits,
		bitsSliceLen: elements.bitsSliceLen,
	}

	newElements = newBitBools
	return
}

func (elements Elements) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
	idxLen := len(idx)
	if elements.Len() < idxLen {
		err = errors.New(fmt.Sprintf("index size %d off elements_int size %d", idxLen, elements.Len()))
		return
	}
	newBitsSliceLen := idxLen >> 4
	if (newBitsSliceLen << 4) != idxLen {
		newBitsSliceLen = newBitsSliceLen + 1
	}
	newBits := make([]uint32, newBitsSliceLen)
	newBitBools := BitBools{
		bits: newBits,
		bitsSliceLen: uint32(newBitsSliceLen),
	}
	newBitBools.clearBits()

	for bitsI, index := range idx {
		value, e  := elements.location(int(index))
		if e != nil {
			err = fmt.Errorf("subset bool elements error: %w", e)
			return
		}
		e = newBitBools.set(bitsI, value)
		if e != nil {
			err = fmt.Errorf("subset bool elements error: %w", e)
			return
		}
	}

	newElements = newBitBools
	return
}

func New(elements []bool) (newElements Elements) {
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
		boolValue := utils.If(value == true, trueValue, falseValue)
		newBitBools.set(bitsI, boolValue.(bitBoolValue))
	}

	newElements = newBitBools
	return
}