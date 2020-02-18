package condition

import "fmt"

const (
	tokenliteral = iota
	tokenOperatorAnd
	tokenOperatorOr
)

const (
	operatorAnd = "&&"
	operatorOr = "||"
)

var precedence = map[string]int{
	"&&": 40,
	"||": 20,
}

type condToken struct {
	cond *CondValue
	tokenType int
}

func (token *condToken) String() string {
	var condString string
	switch token.tokenType {
	case tokenliteral:
		condString = fmt.Sprintf("%s", token.cond)
	case tokenOperatorAnd:
		condString = operatorAnd
	case tokenOperatorOr:
		condString = operatorOr
	}
	return condString
}

type condAST struct {
	tokens []*condToken

	curToken *condToken
	curIndex int

	Err error
}

func (ast *condAST) getNextToken() {
	ast.curIndex++
	if ast.curIndex < len(ast.tokens) {
		ast.curToken = ast.tokens[ast.curIndex]
	} else {
		ast.curToken = nil
	}
}

func (ast *condAST) parseToken() ExprAST {
	if ast.curToken == nil {
		return nil
	}
	switch ast.curToken.tokenType {
	case tokenliteral:
		valExprAST := ValueExprAST{
		    Value: ast.curToken.cond,
		}
		ast.getNextToken()
		return valExprAST
	case tokenOperatorOr:
		binExprAST := BinaryExprAST{
			Op: operatorOr,
			Lhs: nil,
			Rhs: nil,
		}
		ast.getNextToken()
		return binExprAST
	case tokenOperatorAnd:
		binExprAST := BinaryExprAST{
			Op: operatorAnd,
			Lhs: nil,
			Rhs: nil,
		}
		ast.getNextToken()
		return binExprAST
	}
	return nil
}

func (ast *condAST) parseExpr() (root ExprAST) {
	lhs := ast.parseToken()
	operator := ast.parseToken()
	if operator == nil {
		root = lhs
		return
	}
	rhs := ast.parseToken()
	stack := NewExprStack()
	stack.Push(lhs, operator, rhs)
	for opExpr := ast.parseToken(); opExpr != nil; opExpr = ast.parseToken(){
		op := opExpr.(BinaryExprAST)
		prec := op.getPrecedence()
		fmt.Printf("%v\n", op)
		fmt.Printf("%v\n", stack.Peek())
		fmt.Printf("%v\n", stack.PeekSecond())
		prevOp := stack.PeekSecond().(BinaryExprAST)
		prevPrec := prevOp.getPrecedence()
		for stack.Len() > 2 && prec < prevPrec {
			rhs := stack.Pop()
			operator := stack.Pop()
			lhs := stack.Pop()
			oper := operator.(BinaryExprAST)
			oper.Lhs = lhs
			oper.Rhs = rhs
			stack.Push(oper)
		}
		valExpr := ast.parseToken()
		stack.Push(op, valExpr)
	}
	root = stack.Pop()
	for stack.Len() > 1 {
		opExpr := stack.Pop()
		op := opExpr.(BinaryExprAST)
		lhs := stack.Pop()
		op.Lhs = lhs
		op.Rhs = root
		root = op
	}
	return
}

func NewAST() *condAST {
	ast := new(condAST)
	ast.curIndex = -1
	return ast
}

type ExprAST interface {
	toStr() string
}

type ValueExprAST struct {
	Value *CondValue
}

func (valExprAST ValueExprAST) toStr() string {
	return fmt.Sprintf(
		"ValueExprtAST: %s",
		valExprAST.Value)
}

type BinaryExprAST struct {
	Op string
	Lhs,
	Rhs ExprAST
}

func (binExprAST BinaryExprAST) toStr() string {
	return fmt.Sprintf(
		"BinaryExprAST: (%s %s %s)",
		binExprAST.Op,
		binExprAST.Lhs.toStr(),
		binExprAST.Rhs.toStr(),
		)
}

func (binExprAST BinaryExprAST)  getPrecedence() int {
	return precedence[binExprAST.Op]
}