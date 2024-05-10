package drivers

import (
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/logs"
)

type Redis struct {
	Len   uint
	K     uint
	Key   string
	Redis contracts.RedisConnection
}

func (driver *Redis) Add(bytes []byte) {
	h := baseHashes(bytes)
	for i := uint(0); i < driver.K; i++ {
		driver.set(driver.location(h, i))
	}
}

// location returns the ith hashed location using the four base hash values
func (driver *Redis) location(h [4]uint64, i uint) int64 {
	return int64(location(h, i) % uint64(driver.Len))
}

func (driver *Redis) test(location int64) bool {
	v, err := driver.Redis.GetBit(driver.Key, location)
	if v == 0 || err != nil {
		return false
	}
	return true
}

func (driver *Redis) set(location int64) {
	_, err := driver.Redis.SetBit(driver.Key, location, 1)
	if err != nil {
		logs.WithError(err).WithField("Key", driver.Key).Error("Redis.set: Failed to save bit")
	}
}

func (driver *Redis) AddString(str string) {
	driver.Add([]byte(str))
}

func (driver *Redis) Test(bytes []byte) bool {
	h := baseHashes(bytes)
	for i := uint(0); i < driver.K; i++ {
		if !driver.test(driver.location(h, i)) {
			return false
		}
	}
	return true
}

// TestAndAdd is the equivalent to calling Test(data) then Add(data).
// Returns the result of Test.
func (driver *Redis) TestAndAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < driver.K; i++ {
		l := driver.location(h, i)
		if !driver.test(l) {
			present = false
		}
		driver.set(l)
	}
	return present
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// Returns the result of Test.
func (driver *Redis) TestAndAddString(data string) bool {
	return driver.TestAndAdd([]byte(data))
}

// TestOrAdd is the equivalent to calling Test(data) then if not present Add(data).
// Returns the result of Test.
func (driver *Redis) TestOrAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < driver.K; i++ {
		l := driver.location(h, i)
		if !driver.test(l) {
			present = false
			driver.set(l)
		}
	}
	return present
}

// TestOrAddString is the equivalent to calling Test(string) then if not present Add(string).
// Returns the result of Test.
func (driver *Redis) TestOrAddString(data string) bool {
	return driver.TestOrAdd([]byte(data))
}

func (driver *Redis) TestString(str string) bool {
	return driver.Test([]byte(str))
}

func (driver *Redis) Clear() {
	_, err := driver.Redis.Del(driver.Key)
	if err != nil {
		logs.WithError(err).WithField("Key", driver.Key).Error("Redis.Clear: failed to delete")
	}
}

func (driver *Redis) Size() uint {
	return driver.Len
}

func (driver *Redis) Count() uint {
	count, _ := driver.Redis.BitCount(driver.Key, &contracts.BitCount{
		Start: 0,
		End:   int64(driver.Len),
	})
	return uint(count)
}

func (driver *Redis) Load() {
}

func (driver *Redis) Save() {
}
