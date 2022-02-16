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

func FileDriver(config contracts.Fields) contracts.BloomFilter {
	size, k := EstimateParameters(
		uint(utils.GetIntField(config, "size", 0)),
		utils.GetFloat64Field(config, "k", 0),
	)
	return &File{
		size:     max(size, 1),
		k:        max(k, 1),
		bits:     bitset.New(size),
		filepath: config["filepath"].(string),
	}
}

func EstimateParameters(n uint, p float64) (m uint, k uint) {
	m = uint(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return
}

func max(x, y uint) uint {
	if x > y {
		return x
	}
	return y
}

// baseHashes returns the four hash values of data that are used to create k
// hashes
func baseHashes(data []byte) [4]uint64 {
	var d hash.Digest128 // murmur hashing
	hash1, hash2, hash3, hash4 := d.Sum256(data)
	return [4]uint64{
		hash1, hash2, hash3, hash4,
	}
}

type File struct {
	size uint
	k    uint
	bits *bitset.BitSet

	filepath string
}

func (f *File) Add(bytes []byte) {
	h := baseHashes(bytes)
	for i := uint(0); i < f.k; i++ {
		f.bits.Set(f.location(h, i))
	}
}

// location returns the ith hashed location using the four base hash values
func location(h [4]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
}

// location returns the ith hashed location using the four base hash values
func (f *File) location(h [4]uint64, i uint) uint {
	return uint(location(h, i) % uint64(f.size))
}

func (f *File) AddString(str string) {
	f.Add([]byte(str))
}

func (f *File) Test(bytes []byte) bool {
	h := baseHashes(bytes)
	for i := uint(0); i < f.k; i++ {
		if !f.bits.Test(f.location(h, i)) {
			return false
		}
	}
	return true
}

// TestAndAdd is the equivalent to calling Test(data) then Add(data).
// Returns the result of Test.
func (f *File) TestAndAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		l := f.location(h, i)
		if !f.bits.Test(l) {
			present = false
		}
		f.bits.Set(l)
	}
	return present
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// Returns the result of Test.
func (f *File) TestAndAddString(data string) bool {
	return f.TestAndAdd([]byte(data))
}

// TestOrAdd is the equivalent to calling Test(data) then if not present Add(data).
// Returns the result of Test.
func (f *File) TestOrAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		l := f.location(h, i)
		if !f.bits.Test(l) {
			present = false
			f.bits.Set(l)
		}
	}
	return present
}

// TestOrAddString is the equivalent to calling Test(string) then if not present Add(string).
// Returns the result of Test.
func (f *File) TestOrAddString(data string) bool {
	return f.TestOrAdd([]byte(data))
}

func (f *File) TestString(str string) bool {
	return f.Test([]byte(str))
}

func (f *File) Clear() {
	f.bits.ClearAll()
}

func (f *File) Size() uint {
	return uint(f.bits.BinaryStorageSize())
}

func (f *File) Count() uint {
	return f.bits.Count()
}

func (f *File) Load() {
	file, err := os.Open(f.filepath)
	if err != nil {
		logs.WithError(err).Error("File.Load: file open failed")
		return
	}

	_, err = f.ReadFrom(file)

	if err != nil {
		logs.WithError(err).Error("File.ReadFrom: file read failed")
		return
	}
}

func (f *File) Save() {
	file, err := os.OpenFile(f.filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		logs.WithError(err).Error("File.Save: file open failed")
		return
	}

	_, err = f.WriteTo(file)

	if err != nil {
		logs.WithError(err).Error("File.WriteTo: file write failed")
		return
	}
}

// WriteTo writes a binary representation of the BloomFilter to an i/o stream.
// It returns the number of bytes written.
func (f *File) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(f.size))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(f.k))
	if err != nil {
		return 0, err
	}
	numBytes, err := f.bits.WriteTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

// ReadFrom reads a binary representation of the BloomFilter (such as might
// have been written by WriteTo()) from an i/o stream. It returns the number
// of bytes read.
func (f *File) ReadFrom(stream io.Reader) (int64, error) {
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
	f.size = uint(m)
	f.k = uint(k)
	f.bits = b
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}
