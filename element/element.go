package element

type Type string

const NaN string = "NaN"

const (
	TypeString Type = "string"
	TypeInt Type = "int"
	TypeFloat Type = "float"
	TypeBool Type = "bool"
	TypeObject Type = "object"
)

type Elements interface {
	Index(int) Element
	Len() int
	Init(int)
}

type Element interface {
	Type() Type
	Set(interface{})
	String() string
}