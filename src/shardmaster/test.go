package shardmaster

import (
	"fmt"
)

func main() {
	arr := []int{1, 2, 3, 4, 5, 6}
	var shards [10]int
	for i := 0; i < 10; {
		for j := 0; j < len(arr) && i < 10; j++ {
			shards[i] = arr[j]
			i++
		}
	}
	for k, v := range shards {
		fmt.Println(k, v)
	}
}
