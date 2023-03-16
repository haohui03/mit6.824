package raft

import (
	"fmt"
	"reflect"
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func SEntries(entries []*Entry) []string {
	str := []string{}
	for _, v := range entries {
		subStr := fmt.Sprintf("%v", reflect.ValueOf(v))
		str = append(str, subStr)
	}
	return str
}
