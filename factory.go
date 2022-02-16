package bloomfilter

import (
	"errors"
	"fmt"
	"github.com/goal-web/bloomfilter/drivers"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/exceptions"
	"github.com/goal-web/supports/logs"
	"github.com/goal-web/supports/utils"
	"strings"
	"sync"
)

var DriverNotDefineErr = errors.New("driver not defined")
var FilterNotDefineErr = errors.New("filter not defined")

func NewFactory(config Config, redis contracts.RedisFactory) contracts.BloomFactory {
	return &Factory{
		drivers: map[string]contracts.BloomFilterDriver{
			"file": drivers.FileDriver,
			"redis": func(name string, config contracts.Fields) contracts.BloomFilter {
				size, k := drivers.EstimateParameters(
					uint(utils.GetIntField(config, "size", 10000)),
					utils.GetFloat64Field(config, "k", 1),
				)
				return &drivers.Redis{
					Len:   drivers.Max(size, 1),
					K:     drivers.Max(k, 1),
					Key:   strings.ReplaceAll(utils.GetStringField(config, "key", fmt.Sprintf("bloomfilter:%s", name)), "{name}", name),
					Redis: redis.Connection(utils.GetStringField(config, "connection")),
				}
			},
		},
		filters: sync.Map{},
		config:  config,
	}
}

type Factory struct {
	drivers map[string]contracts.BloomFilterDriver
	filters sync.Map
	config  Config
}

func (factory *Factory) Start() (err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = exceptions.WithRecover(err, contracts.Fields{"config": factory.config})
		}
	}()

	for name, _ := range factory.config.Filters {
		factory.Filter(name).Load()
	}
	return
}

func (factory *Factory) Close() {
	for name, _ := range factory.config.Filters {
		factory.Filter(name).Save()
	}
}

func (factory *Factory) Extend(name string, driver contracts.BloomFilterDriver) {
	factory.drivers[name] = driver
}

func (factory *Factory) Filter(name string) contracts.BloomFilter {
	value, ok := factory.filters.Load(name)
	if ok {
		return value.(contracts.BloomFilter)
	}

	config := factory.config.Filters[name]
	if config == nil {
		logs.WithError(FilterNotDefineErr).WithField("name", name).Error("bloomfilter.Factory.Filter: ")
		panic(FilterNotDefineErr)
	}

	driver := utils.GetStringField(config, "driver")
	if factory.drivers[driver] == nil {
		logs.WithError(DriverNotDefineErr).WithField("name", name).WithFields(config).Error("bloomfilter.Factory.Filter: ")
		panic(DriverNotDefineErr)
	}

	var filter = factory.drivers[driver](name, config)
	factory.filters.Store(name, filter)

	return filter
}
