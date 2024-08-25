package data_manager

import (
	"testing"
)

type TestCache[T any] struct {
	//Cache is the actual cache
	Cache map[Long]T
}

func Test_General(t *testing.T) {
	cache := TestCache[string]{}
	cache.Cache = make(map[Long]string)

	record, exist := cache.Cache[0]
	if !exist {
		t.Error("Record should exist")
	}
	print(record)

}
