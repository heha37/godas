package condition

type (
	exprStack struct {
		top *node
		length int
	}
	node struct {
		value ExprAST
		prev *node
	}
)

func NewExprStack() *exprStack {
	return &exprStack{nil, 0}
}

func (this *exprStack) Len() int {
	return this.length
}

func (this *exprStack) Peek() ExprAST {
    if this.length == 0 {
    	return nil
	}
	return this.top.value
}

func (this *exprStack) PeekSecond() ExprAST {
	if this.top.prev == nil {
		return nil
	}
	return this.top.prev.value
}

func (this *exprStack) Pop() ExprAST {
	if this.length == 0 {
		return nil
	}
	n := this.top
	this.top = n.prev
	this.length--
	return n.value
}

func (this *exprStack) Push(values ...ExprAST) {
	for _, val := range values {
		n := &node{
			value: val,
			prev: this.top,
		}
		this.top = n
		this.length++
	}
}