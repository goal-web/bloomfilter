package bloomfilter

import (
	"github.com/goal-web/contracts"
)

type ServiceProvider struct {
	app contracts.Application
}

func (this *ServiceProvider) Register(application contracts.Application) {
	this.app = application
	application.Singleton("bloom.factory", func(config contracts.Config) contracts.BloomFactory {
		return NewFactory(config.Get("bloomfilter").(Config))
	})

	application.Singleton("bloom.filter", func(factory contracts.BloomFactory) contracts.BloomFilter {
		return factory.Filter(factory.(*Factory).config.Default)
	})
}

func (this *ServiceProvider) Start() error {
	return this.app.Call(func(factory contracts.BloomFactory) error {
		return factory.Start()
	})[0].(error)
}

func (this *ServiceProvider) Stop() {
}
