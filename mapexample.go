package main

import (
	"strings"
)

func MapFunc(file string, value string) [][2]string {
	words := strings.Fields(value)
	res := make([][2]string, len(words))
	for index, w := range words {
		kv := [2]string{w, ""}
		res[index] = kv
	}
	return res
}
