package bloomfilter

import (
	"github.com/goal-web/contracts"
)

type serviceProvider struct {
	app contracts.Application
}

func NewService() contracts.ServiceProvider {
	return &serviceProvider{}
}

func (provider *serviceProvider) Register(application contracts.Application) {
	provider.app = application

	application.Singleton("bloom.factory", func(config contracts.Config, redis contracts.RedisFactory) contracts.BloomFactory {
		return NewFactory(config.Get("bloomfilter").(Config), redis)
	})

	application.Singleton("bloom.filter", func(factory contracts.BloomFactory) contracts.BloomFilter {
		return factory.Filter(factory.(*Factory).config.Default)
	})
}

func (provider *serviceProvider) Start() (err error) {
	provider.app.Call(func(factory contracts.BloomFactory) {
		err = factory.Start()
	})
	return err
}

func (provider *serviceProvider) Stop() {
	provider.app.Call(func(factory contracts.BloomFactory) {
		factory.Close()
	})
}
