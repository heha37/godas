package elements_bool

import (
	"errors"
	"fmt"
)

func (elements Elements) clearBits() {
	for i := uint32(0); i < elements.bitsSliceLen; i++ {
		elements.bits[i] = chunkNullValue
	}
}

func calculateChunkAndBitIndex(coord int) (chunkI, bitsI int) {
	chunkI = coord >> 4
	bitsI = coord - (chunkI << 4)
	return
}

func (elements Elements) location(coord int) (value bitBoolValue, err error){
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

func (elements Elements) bitSliceLen() int {
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

func (elements Elements) set(coord int, value bitBoolValue) (err error){
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