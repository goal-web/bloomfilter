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

func (this *Redis) Add(bytes []byte) {
	h := baseHashes(bytes)
	for i := uint(0); i < this.K; i++ {
		this.set(this.location(h, i))
	}
}

// location returns the ith hashed location using the four base hash values
func (this *Redis) location(h [4]uint64, i uint) int64 {
	return int64(location(h, i) % uint64(this.Len))
}

func (this *Redis) test(location int64) bool {
	v, err := this.Redis.GetBit(this.Key, location)
	if v == 0 || err != nil {
		return false
	}
	return true
}

func (this *Redis) set(location int64) {
	_, err := this.Redis.SetBit(this.Key, location, 1)
	if err != nil {
		logs.WithError(err).WithField("Key", this.Key).Error("Redis.set: Failed to save bit")
	}
}

func (this *Redis) AddString(str string) {
	this.Add([]byte(str))
}

func (this *Redis) Test(bytes []byte) bool {
	h := baseHashes(bytes)
	for i := uint(0); i < this.K; i++ {
		if !this.test(this.location(h, i)) {
			return false
		}
	}
	return true
}

// TestAndAdd is the equivalent to calling Test(data) then Add(data).
// Returns the result of Test.
func (this *Redis) TestAndAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < this.K; i++ {
		l := this.location(h, i)
		if !this.test(l) {
			present = false
		}
		this.set(l)
	}
	return present
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// Returns the result of Test.
func (this *Redis) TestAndAddString(data string) bool {
	return this.TestAndAdd([]byte(data))
}

// TestOrAdd is the equivalent to calling Test(data) then if not present Add(data).
// Returns the result of Test.
func (this *Redis) TestOrAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < this.K; i++ {
		l := this.location(h, i)
		if !this.test(l) {
			present = false
			this.set(l)
		}
	}
	return present
}

// TestOrAddString is the equivalent to calling Test(string) then if not present Add(string).
// Returns the result of Test.
func (this *Redis) TestOrAddString(data string) bool {
	return this.TestOrAdd([]byte(data))
}

func (this *Redis) TestString(str string) bool {
	return this.Test([]byte(str))
}

func (this *Redis) Clear() {
	_, err := this.Redis.Del(this.Key)
	if err != nil {
		logs.WithError(err).WithField("Key", this.Key).Error("Redis.Clear: failed to delete")
	}
}

func (this *Redis) Size() uint {
	return this.Len
}

func (this *Redis) Count() uint {
	count, _ := this.Redis.BitCount(this.Key, &contracts.BitCount{
		Start: 0,
		End:   int64(this.Len),
	})
	return uint(count)
}

func (this *Redis) Load() {
}

func (this *Redis) Save() {
}
