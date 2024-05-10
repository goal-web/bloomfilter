package drivers

import (
	"encoding/binary"
	"github.com/bits-and-blooms/bitset"
	"github.com/goal-web/bloomfilter/hash"
	"github.com/goal-web/contracts"
	"github.com/goal-web/supports/logs"
	"github.com/goal-web/supports/utils"
	"io"
	"math"
	"os"
)

func FileDriver(name string, config contracts.Fields) contracts.BloomFilter {
	size, k := EstimateParameters(
		uint(utils.GetIntField(config, "Len", 0)),
		utils.GetFloat64Field(config, "K", 0),
	)
	return &File{
		name:     name,
		size:     Max(size, 1),
		k:        Max(k, 1),
		bits:     bitset.New(size),
		filepath: config["filepath"].(string),
	}
}

func EstimateParameters(n uint, p float64) (m uint, k uint) {
	m = uint(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return
}

func Max(x, y uint) uint {
	if x > y {
		return x
	}
	return y
}

// baseHashes returns the four hash values of data that are used to create K
// hashes
func baseHashes(data []byte) [4]uint64 {
	var d hash.Digest128 // murmur hashing
	hash1, hash2, hash3, hash4 := d.Sum256(data)
	return [4]uint64{
		hash1, hash2, hash3, hash4,
	}
}

type File struct {
	name string
	size uint
	k    uint
	bits *bitset.BitSet

	filepath string
}

func (driver *File) Add(bytes []byte) {
	h := baseHashes(bytes)
	for i := uint(0); i < driver.k; i++ {
		driver.bits.Set(driver.location(h, i))
	}
}

// location returns the ith hashed location using the four base hash values
func location(h [4]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
}

// location returns the ith hashed location using the four base hash values
func (driver *File) location(h [4]uint64, i uint) uint {
	return uint(location(h, i) % uint64(driver.size))
}

func (driver *File) AddString(str string) {
	driver.Add([]byte(str))
}

func (driver *File) Test(bytes []byte) bool {
	h := baseHashes(bytes)
	for i := uint(0); i < driver.k; i++ {
		if !driver.bits.Test(driver.location(h, i)) {
			return false
		}
	}
	return true
}

// TestAndAdd is the equivalent to calling Test(data) then Add(data).
// Returns the result of Test.
func (driver *File) TestAndAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < driver.k; i++ {
		l := driver.location(h, i)
		if !driver.bits.Test(l) {
			present = false
		}
		driver.bits.Set(l)
	}
	return present
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// Returns the result of Test.
func (driver *File) TestAndAddString(data string) bool {
	return driver.TestAndAdd([]byte(data))
}

// TestOrAdd is the equivalent to calling Test(data) then if not present Add(data).
// Returns the result of Test.
func (driver *File) TestOrAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < driver.k; i++ {
		l := driver.location(h, i)
		if !driver.bits.Test(l) {
			present = false
			driver.bits.Set(l)
		}
	}
	return present
}

// TestOrAddString is the equivalent to calling Test(string) then if not present Add(string).
// Returns the result of Test.
func (driver *File) TestOrAddString(data string) bool {
	return driver.TestOrAdd([]byte(data))
}

func (driver *File) TestString(str string) bool {
	return driver.Test([]byte(str))
}

func (driver *File) Clear() {
	driver.bits.ClearAll()
}

func (driver *File) Size() uint {
	return driver.size
}

func (driver *File) Count() uint {
	return driver.bits.Count()
}

func (driver *File) Load() {
	file, err := os.Open(driver.filepath)
	if err != nil {
		logs.WithError(err).Debug("bloomfilter.drivers.File.Load: file open failed")
		return
	}

	_, err = driver.ReadFrom(file)

	if err != nil {
		logs.WithError(err).Debug("bloomfilter.drivers.File.ReadFrom: file read failed")
		return
	}
}

func (driver *File) Save() {
	file, err := os.OpenFile(driver.filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		logs.WithError(err).Error("bloomfilter.drivers.File.Save: file open failed")
		return
	}

	_, err = driver.WriteTo(file)

	if err != nil {
		logs.WithError(err).Error("bloomfilter.drivers.File.WriteTo: file write failed")
		return
	}
}

// WriteTo writes a binary representation of the BloomFilter to an i/o stream.
// It returns the number of bytes written.
func (driver *File) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(driver.size))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(driver.k))
	if err != nil {
		return 0, err
	}
	numBytes, err := driver.bits.WriteTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

// ReadFrom reads a binary representation of the BloomFilter (such as might
// have been written by WriteTo()) from an i/o stream. It returns the number
// of bytes read.
func (driver *File) ReadFrom(stream io.Reader) (int64, error) {
	var m, k uint64
	err := binary.Read(stream, binary.BigEndian, &m)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &k)
	if err != nil {
		return 0, err
	}
	b := &bitset.BitSet{}
	numBytes, err := b.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	driver.size = uint(m)
	driver.k = uint(k)
	driver.bits = b
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}
