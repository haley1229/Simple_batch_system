package main

import (
	"strconv"
)

func ReduceFunc(key string, values []string) string {
	return strconv.Itoa(len(values))
}
