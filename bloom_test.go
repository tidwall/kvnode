package roam

import (
	"fmt"
	"testing"

	"github.com/willf/bloom"
)

func TestBloom(t *testing.T) {
	filter := bloom.New(10000000, 5) // load of 20, 5 keys
	filter.Add([]byte("Love"))
	println(filter.Test([]byte("Love")))
	println(filter.Test([]byte("Loved")))
	fmt.Printf("%v\n", filter.EstimateFalsePositiveRate(1000000))
}
