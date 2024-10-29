package main

import (
	"fmt"
	"sort"
)

func main() {
	groups := make(map[int][]string)
	groups[8] = []string{"a", "b"}
	groups[9] = []string{"c", "d"}
	groups[10] = []string{"e", "f"}
	groups[11] = []string{"g", "h"}
	groups[12] = []string{"i", "j"}
	gIds := make([]int, len(groups))
	i := 0
	for gId := range groups {
		gIds[i] = gId
		i++
	}
	fmt.Println(gIds)
	sort.Ints(gIds)
	fmt.Println(gIds)
}
