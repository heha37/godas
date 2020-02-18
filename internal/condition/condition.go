package condition

import (
	"errors"
	"fmt"
)

const (
	ConditionTypeSeries = iota
	ConditionTypeDataFrame
)

const (
	ComparatorGT = ">"
	ComparatorLT = "<"
	ComparatorGTE = ">="
	ComparatorEq ="="
	ComparatorLTE = "<="
	ComparatorIn = "in"
	ComparatorIsNan = "is_nan"
)

type CompItem struct {
	Comparator string
	Column string
	Value interface{}
}

type CondValue struct {
	Cond *Condition
	CompItem *CompItem
	IsNot bool
}

func (condVal *CondValue) CompareInt(leftVal int) (compareResult bool, err error) {
	item := condVal.CompItem
	rightVal, ok := item.Value.(int)
	if !ok {
		err = errors.New(fmt.Sprintf("can't convert value %v to int", item.Value))
		return
	}
	switch item.Comparator {
	case ComparatorGT:
		compareResult = leftVal > rightVal
	case ComparatorEq:
		compareResult = leftVal == rightVal
	case ComparatorLT:
		compareResult = leftVal < rightVal
	case ComparatorGTE:
		compareResult = leftVal >= rightVal
	case ComparatorLTE:
		compareResult = leftVal <= rightVal
	}
	return
}

func (condVal *CondValue) CompareFloat64(leftVal float64) (compareResult bool, err error) {
	item := condVal.CompItem
	rightVal, ok := item.Value.(float64)
	if !ok {
		err = errors.New(fmt.Sprintf("can't convert value %v to float64", item.Value))
		return
	}
	switch item.Comparator {
	case ComparatorGT:
		compareResult = leftVal > rightVal
	case ComparatorLT:
		compareResult = leftVal < rightVal
	}
	return
}

func (condVal *CondValue) String() string {
	var condString string
	if condVal.CompItem != nil && condVal.IsNot {
		comp := condVal.CompItem
		condString = fmt.Sprintf("!(%s %s %v)",
			comp.Column, comp.Comparator, comp.Value)
	}
	if condVal.CompItem != nil && !condVal.IsNot {
		comp := condVal.CompItem
		condString = fmt.Sprintf("(%s %s %v)",
			comp.Column, comp.Comparator, comp.Value)
	}
	if condVal.Cond != nil {
		condString = fmt.Sprintf("%s", condVal.Cond)
	}
	return condString
}

type Condition struct {
	ast *condAST
	condType int
}

func (cond *Condition) String() string {
	var condString string
	tokens := cond.ast.tokens
	for _, token := range tokens {
		condString += fmt.Sprintf("%s", token)
	}
	return condString
}

func (cond *Condition) And(comparator string, value interface{}, columns ...string) {
	ast := cond.ast
	var column string
	if len(columns) > 0 {
		column = columns[0]
	}
	if ast.curIndex != -1 {
		tokenOperator := &condToken{
			tokenType: tokenOperatorAnd,
		}
		ast.tokens = append(ast.tokens, tokenOperator)
	}
	cmp := &CompItem{
		Comparator: comparator,
		Column:     column,
		Value:      value,
	}
	condVal := &CondValue{
		CompItem: cmp,
	}
	tokenLiteral := &condToken{
		cond: condVal,
		tokenType: tokenliteral,
	}
	ast.tokens = append(ast.tokens, tokenLiteral)

	if ast.curIndex == -1 {
		ast.curIndex = 0
		ast.curToken = ast.tokens[ast.curIndex]
	}
}

func (cond *Condition) Or(comparator string, value interface{}, columns ...string) {
	ast := cond.ast
	var column string
	if len(columns) > 0 {
		column = columns[0]
	}

	if ast.curIndex != -1 {
		tokenOperator := &condToken{
			tokenType: tokenOperatorOr,
		}
		ast.tokens = append(ast.tokens, tokenOperator)
	}
	cmp := &CompItem{
		Comparator: comparator,
		Column:     column,
		Value:      value,
	}
	condVal := &CondValue{
		CompItem: cmp,
	}
	tokenLiteral := &condToken{
		cond: condVal,
		tokenType: tokenliteral,
	}
	ast.tokens = append(ast.tokens, tokenLiteral)

	if ast.curIndex == -1 {
		ast.curIndex = 0
		ast.curToken = ast.tokens[ast.curIndex]
	}
}

func (cond *Condition) Prepare() ExprAST {
	return cond.ast.parseExpr()
}

func NewCondition(condType int) *Condition {
	ast := NewAST()
	return &Condition{
		ast: ast,
		condType: condType,
	}
}