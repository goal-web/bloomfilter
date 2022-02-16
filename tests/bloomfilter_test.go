package tests

import (
	"fmt"
	"github.com/goal-web/bloomfilter"
	"github.com/goal-web/contracts"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFilter(t *testing.T) {
	var factory = bloomfilter.NewFactory(bloomfilter.Config{
		Default: "default",
		Filters: bloomfilter.Filters{
			"default": contracts.Fields{
				"driver":   "file",
				"size":     1000,
				"k":        0.01,
				"filepath": "/Users/qbhy/project/go/goal-web/bloomfilter/tests/default",
			},
		},
	})

	assert.Nil(t, factory.Start())
	defer factory.Close()

	var filter = factory.Filter("default")

	for i := 0; i < 100; i++ {
		//filter.AddString(fmt.Sprintf("goal%d", i))
		filter.TestString(fmt.Sprintf("goal%d", i))
	}

	assert.False(t, filter.TestString("1"))

}

/**
goos: darwin
goarch: amd64
pkg: github.com/goal-web/bloomfilter/tests
cpu: Intel(R) Core(TM) i7-7660U CPU @ 2.50GHz
BenchmarkFilterAdd
BenchmarkFilterAdd-4   	 4259469	       244.0 ns/op
*/
func BenchmarkFilterAdd(b *testing.B) {
	var factory = bloomfilter.NewFactory(bloomfilter.Config{
		Default: "default",
		Filters: bloomfilter.Filters{
			"default": contracts.Fields{
				"driver":   "file",
				"size":     1000,
				"k":        0.01,
				"filepath": "/Users/qbhy/project/go/goal-web/bloomfilter/tests/default",
			},
		},
	})

	var filter = factory.Filter("default")

	for i := 0; i < b.N; i++ {
		filter.AddString(fmt.Sprintf("%d", i))
	}
}

/**
goos: darwin
goarch: amd64
pkg: github.com/goal-web/bloomfilter/tests
cpu: Intel(R) Core(TM) i7-7660U CPU @ 2.50GHz
BenchmarkFilterTest
BenchmarkFilterTest-4   	 6149362	       183.2 ns/op
*/
func BenchmarkFilterTest(b *testing.B) {
	var factory = bloomfilter.NewFactory(bloomfilter.Config{
		Default: "default",
		Filters: bloomfilter.Filters{
			"default": contracts.Fields{
				"driver":   "file",
				"size":     1000,
				"k":        0.01,
				"filepath": "/Users/qbhy/project/go/goal-web/bloomfilter/tests/default",
			},
		},
	})

	var filter = factory.Filter("default")

	for i := 0; i < b.N; i++ {
		filter.TestString(fmt.Sprintf("%d", i))
	}
}

/**
goos: darwin
goarch: amd64
pkg: github.com/goal-web/bloomfilter/tests
cpu: Intel(R) Core(TM) i7-7660U CPU @ 2.50GHz
BenchmarkFilterTestAndAddString
BenchmarkFilterTestAndAddString-4   	 4384441	       301.1 ns/op
*/
func BenchmarkFilterTestAndAddString(b *testing.B) {
	var factory = bloomfilter.NewFactory(bloomfilter.Config{
		Default: "default",
		Filters: bloomfilter.Filters{
			"default": contracts.Fields{
				"driver":   "file",
				"size":     1000,
				"k":        0.01,
				"filepath": "/Users/qbhy/project/go/goal-web/bloomfilter/tests/default",
			},
		},
	})

	var filter = factory.Filter("default")

	for i := 0; i < b.N; i++ {
		filter.TestAndAddString(fmt.Sprintf("%d", i))
	}
}

/**
goos: darwin
goarch: amd64
pkg: github.com/goal-web/bloomfilter/tests
cpu: Intel(R) Core(TM) i7-7660U CPU @ 2.50GHz
BenchmarkFilterTestOrAddString
BenchmarkFilterTestOrAddString-4   	 3447021	       300.2 ns/op
*/
func BenchmarkFilterTestOrAddString(b *testing.B) {
	var factory = bloomfilter.NewFactory(bloomfilter.Config{
		Default: "default",
		Filters: bloomfilter.Filters{
			"default": contracts.Fields{
				"driver":   "file",
				"size":     1000,
				"k":        0.01,
				"filepath": "/Users/qbhy/project/go/goal-web/bloomfilter/tests/default",
			},
		},
	})

	var filter = factory.Filter("default")

	for i := 0; i < b.N; i++ {
		filter.TestOrAddString(fmt.Sprintf("%d", i))
	}
}

/**
goos: darwin
goarch: amd64
pkg: github.com/goal-web/bloomfilter/tests
cpu: Intel(R) Core(TM) i7-7660U CPU @ 2.50GHz
BenchmarkFilterAddAndTest
BenchmarkFilterAddAndTest-4   	 2426726	       425.2 ns/op
*/
func BenchmarkFilterAddAndTest(b *testing.B) {
	var factory = bloomfilter.NewFactory(bloomfilter.Config{
		Default: "default",
		Filters: bloomfilter.Filters{
			"default": contracts.Fields{
				"driver":   "file",
				"size":     1000,
				"k":        0.01,
				"filepath": "/Users/qbhy/project/go/goal-web/bloomfilter/tests/default",
			},
		},
	})

	var filter = factory.Filter("default")

	for i := 0; i < b.N; i++ {
		str := fmt.Sprintf("%d", i)
		filter.AddString(str)
		filter.TestString(str)
	}
}
