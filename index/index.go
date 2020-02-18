package index

type IndexInt []uint32
type IndexBool []bool

func (indexBool IndexBool) Or(otherIndexBool IndexBool) {
	indexLen := len(indexBool)
	for i := 0; i < indexLen; i++ {
		indexBool[i] = indexBool[i] || otherIndexBool[i]
	}
}

func (indexBool IndexBool) And(otherIndexBool IndexBool) {
	indexLen := len(indexBool)
	for i := 0; i < indexLen; i++ {
		indexBool[i] = indexBool[i] && otherIndexBool[i]
	}
}