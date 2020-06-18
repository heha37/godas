package elements

import (
	"errors"
	"fmt"
	"github.com/hunknownz/godas/condition"
	"github.com/hunknownz/godas/index"
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

func (element ElementValue) Bool() (bool, error) {
	if s, ok := (element.Value).(bool); ok {
		return s, nil
	}
	return false, errors.New("type assertion to bool failed")
}

func (element ElementValue) Int() (int64, error) {
	if s, ok := (element.Value).(int64); ok {
		return s, nil
	}
	return int64(0), errors.New("type assertion to int failed")
}

func (element ElementValue) Float() (float64, error) {
	if s, ok := (element.Value).(float64); ok {
		return s, nil
	}
	return float64(0), errors.New("type assertion to float failed")
}

func (element ElementValue) Interface() (interface{}, error) {
	if s, ok := (element.Value).(interface{}); ok {
		return s, nil
	}
	return interface{}(nil), errors.New("type assertion to object faile")
}

func (element ElementValue) MustBool(args ...bool) bool {
	var def bool

	switch len(args) {
	case 0:
	case 1:
		def = args[0]
	default:
		log.Panicf("MustBool() received too many arguments %d", len(args))
	}

	s, err := element.Bool()
	if err == nil {
		return s
	}

	return def
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

func (element ElementValue) MustInt(args ...int64) int64 {
	var def int64

	switch len(args) {
	case 0:
	case 1:
		def = args[0]
	default:
		log.Panicf("MustInt() received too many arguments %d", len(args))
	}

	s, err := element.Int()
	if err == nil {
		return s
	}

	return def
}

func (element ElementValue) MustFloat(args ...float64) float64 {
	var def float64

	switch len(args) {
	case 0:
	case 1:
		def = args[0]
	default:
		log.Panicf("MustFloat() received too many arguments %d", len(args))
	}

	s, err := element.Float()
	if err == nil {
		return s
	}

	return def
}

func (element ElementValue) MustInterface(args ...interface{}) interface{} {
	var def interface{}

	switch len(args) {
	case 0:
	case 1:
		def = args[0]
	default:
		log.Panicf("MustInterface() received too many arguments %d", len(args))
	}

	s, err := element.Interface()
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
	Swap(i, j int)
	Append(copy bool, values ...interface{}) (newElements Elements, err error)
}