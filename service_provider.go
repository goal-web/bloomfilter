package bloomfilter

import (
	"github.com/goal-web/contracts"
)

type ServiceProvider struct {
}

func (s ServiceProvider) Register(application contracts.Application) {

}

func (s ServiceProvider) Start() error {
	return nil
}

func (s ServiceProvider) Stop() {
}
