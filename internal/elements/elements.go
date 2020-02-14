package elements

import (
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/types"
)

type Elements interface {
	Type() types.Type
	String() string
	Len() int
	Copy() (newElements Elements)
	Subset(index.IndexInt) (newElements Elements, err error)
	IsNaN() []bool
}

