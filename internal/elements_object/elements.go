package elements_object

func NewElementsObject(elements []interface{}) (newElements ElementsObject) {
	newElements = ElementsObject{
		itemsLen: len(elements),
		items: elements,
	}
	return
}
