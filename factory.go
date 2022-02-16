package bloomfilter

import (
	"errors"
	"github.com/goal-web/bloomfilter/drivers"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/exceptions"
	"github.com/goal-web/supports/logs"
	"github.com/goal-web/supports/utils"
	"sync"
)

var DriverNotDefineErr = errors.New("driver not defined")
var FilterNotDefineErr = errors.New("filter not defined")

func NewFactory(config Config) contracts.BloomFactory {
	return &Factory{
		drivers: map[string]contracts.BloomFilterDriver{
			"file": drivers.FileDriver,
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

	var filter = factory.drivers[driver](config)
	factory.filters.Store(name, filter)

	return filter
}
