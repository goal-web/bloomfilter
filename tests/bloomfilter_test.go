package tests

import (
	"github.com/bits-and-blooms/bloom/v3"
	"testing"
)

func TestFilter(t *testing.T) {
	filter := bloom.NewWithEstimates(1000000, 0.01)
	filter.Add([]byte(""))

	filter.Test([]byte(""))
}
