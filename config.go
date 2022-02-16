package bloomfilter

import "github.com/goal-web/contracts"

type Config struct {
	Default string

	Filters Filters
}

type Filters map[string]contracts.Fields
