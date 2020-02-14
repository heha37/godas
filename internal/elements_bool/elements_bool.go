package elements_bool

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/elements"
	"github.com/hunknownz/godas/types"
)

type ElementsBool = BitBools

func (elements ElementsBool) Type() (sType types.Type) {
	return types.TypeBool
}

func (elements ElementsBool) Len() int {
	return elements.bitSliceLen()
}

func (elements ElementsBool) String() string {
	return fmt.Sprint(elements)
}

func (elements ElementsBool) Copy() (newElements elements.Elements) {
	newBits := make([]uint32, elements.bitsSliceLen)
	copy(newBits, elements.bits)
	newBitBools := BitBools{
		bits: newBits,
		bitsSliceLen: elements.bitsSliceLen,
	}

	newElements = newBitBools
	return
}

func (elements ElementsBool) Subset(idx index.IndexInt) (newElements elements.Elements, err error) {
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

func (elements ElementsBool) clearBits() {
	for i := uint32(0); i < elements.bitsSliceLen; i++ {
		elements.bits[i] = chunkNullValue
	}
}

func calculateChunkAndBitIndex(coord int) (chunkI, bitsI int) {
	chunkI = coord >> 4
	bitsI = coord - (chunkI << 4)
	return
}

func (elements ElementsBool) location(coord int) (value bitBoolValue, err error){
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	bitSliceLen := elements.bitSliceLen()
	if coord >= bitSliceLen {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, bitSliceLen))
		return
	}

	chunkI, bitsI := calculateChunkAndBitIndex(coord)
	chunk := elements.bits[chunkI]
	chunk = chunk & bitMasks[bitsI]
	if chunk == 0 {
		value = falseValue
	} else if (chunk ^ bitMasks[bitsI]) == 0 {
		value = trueValue
	} else if (chunk ^ bitNaNMasks[bitsI]) == 0 {
		value = nanValue
	} else {
		value = nullValue
	}
	return
}

func (elements ElementsBool) bitSliceLen() int {
	i := elements.bitsSliceLen - 1
	preLen := int(i << 4)
	lastChunk := elements.bits[i]
	var maskI int
	for maskI = 0; maskI < chunkSize; maskI++ {
		isNullHead := (lastChunk & bitMasks[maskI]) ^ bitNullMasks[maskI]
		if isNullHead == 0 {
			break
		}
	}
	sufLen := maskI

	return preLen + sufLen
}

func (elements ElementsBool) set(coord int, value bitBoolValue) (err error){
	if coord < 0 {
		err = errors.New(fmt.Sprintf("invalid index %d (index must be non-negative)", coord))
		return
	}
	bitSliceLen := elements.bitSliceLen()
	if coord >= bitSliceLen {
		err = errors.New(fmt.Sprintf("invalid index %d (out of bounds for %d-element container)", coord, bitSliceLen))
		return
	}

	chunkI, bitsI := calculateChunkAndBitIndex(coord)
	chunk := elements.bits[chunkI]
	switch value {
	case falseValue:
		chunk = chunk &^ bitMasks[bitsI]
	case nanValue:
		chunk = (chunk &^ bitMasks[bitsI]) | bitNaNMasks[bitsI]
	case nullValue:
		chunk = (chunk &^ bitMasks[bitsI]) | bitNullMasks[bitsI]
	case trueValue:
		chunk = (chunk &^ bitMasks[bitsI]) | bitMasks[bitsI]
	default:
		err = errors.New(fmt.Sprintf("invalid bit bool value %d", value))
		return
	}
	elements.bits[chunkI] = chunk
	return
}