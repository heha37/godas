package elements

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/index"
	"github.com/hunknownz/godas/internal/condition"
	"github.com/hunknownz/godas/types"
	"log"
)

type ElementValue struct {
	Value interface{}
	Type types.Type
	IsNaN bool

	Err error
}

func (element ElementValue) String() (string, error) {
	if s, ok := (element.Value).(string); ok {
		return s, nil
	}
	return "", errors.New("type assertion to string failed")
}

func (element ElementValue) MustString(args ...string) string {
	var def string

	switch len(args) {
	case 0:
	case 1:
		def = args[0]
	default:
		log.Panicf("MustString() received too many arguments %d", len(args))
	}

	s, err := element.String()
	if err == nil {
		return s
	}

	return def
}

func (element ElementValue) Compare(cond *condition.CondValue) (result bool, err error) {
	if cond.Cond != nil {
		expr := cond.Cond.Prepare()
		result = element.EvaluateCondition(expr)
	} else {
		switch element.Type {
		case types.TypeInt:
			leftVal := element.Value.(int64)
			var e error
			result, e = cond.CompareInt(leftVal)
			if e != nil {
				err = fmt.Errorf("compare error: %w", e)
				return
			}
		case types.TypeFloat:
			leftVal := element.Value.(float64)
			var e error
			result, e = cond.CompareFloat64(leftVal)
			if e != nil {
				err = fmt.Errorf("compare error: %s", e)
				return
			}
		}
	}
	return
}

func (element ElementValue) EvaluateCondition(expr condition.ExprAST) bool {
	var l, r bool
	switch expr.(type) {
	case condition.BinaryExprAST:
		ast := expr.(condition.BinaryExprAST)
		l = element.EvaluateCondition(ast.Lhs)
		if ast.Op == "&&" && !l {
			return false
		}
		if ast.Op == "||" && l {
			return true
		}
		r = element.EvaluateCondition(ast.Rhs)
		switch ast.Op {
		case "&&":
			return l && r
		case "||":
			return l || r
		}
	case condition.ValueExprAST:
		cond := expr.(condition.ValueExprAST).Value
		result, err := element.Compare(cond)
		if err != nil {
			element.Err = err
			return false
		}
		return result
	}
	return true
}

type Elements interface {
	Type() types.Type
	String() string
	Len() int
	Copy() (newElements Elements)
	Subset(index.IndexInt) (newElements Elements, err error)
	IsNaN() []bool
	Location(int) (ElementValue, error)
}

