package bloomfilter

import (
	"sync"

	"github.com/goal-web/application"
	"github.com/goal-web/contracts"
)

var singleton contracts.BloomFactory
var once sync.Once

func Default() contracts.BloomFactory {
	once.Do(func() {
		singleton = application.Get("bloomfilter").(contracts.BloomFactory)
	})

	return singleton
}

func Extend(name string, driver contracts.BloomFilterDriver) {
	Default().Extend(name, driver)
}

func Filter(name string) contracts.BloomFilter {
	return Default().Filter(name)
}

func Start() error {
	return Default().Start()
}

func Close() {
	Default().Close()
}
