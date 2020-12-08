package main

import "testing"

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func TestRemoveDuuplicatesUnordered(t *testing.T) {
	var strings []string

	strings = append(strings, "abcd")
	strings = append(strings, "aaaa")
	strings = append(strings, "cccc")
	strings = append(strings, "dddd")
	strings = append(strings, "cccc")
	strings = append(strings, "aaaa")
	strings = append(strings, "cccc")

	result := removeDuplicatesUnordered(strings)

	if len(result) != 4 || !stringInSlice("abcd", result) || !stringInSlice("aaaa", result) ||
		!stringInSlice("dddd", result) || !stringInSlice("cccc", result) {
		t.Error()
	}
}
