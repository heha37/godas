package internal

import (
	"reflect"
)

func ArrayContain(array, elem interface{}) (ok bool, position int){
	ok = false
	vArray := reflect.ValueOf(array)
	for i := 0; i < vArray.Len(); i++ {
		if ObjectEqual(vArray.Index(i).Interface(), elem) {
			ok, position = true, i
			return
		}
	}
	return
}

func ObjectEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}
	return reflect.DeepEqual(a, b)
}