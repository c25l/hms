package hmsketch

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/dchest/siphash"
	hist "gitlab.com/c25l/MSFStore"
)

var (
	sipConst = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	hMux     sync.Mutex
)

// the  HMSketch implementation is high-accuracy and, while small, is not of fixed memory.
type HMSketch struct {
	Resolution int
	Max        int
	Index      map[int64]int
	Registers  []hist.Histogram
}

// global accesses the global state
func (m HMSketch) global() hist.Histogram {
	hMux.Lock()
	location := hash("__global__", "__global__")
	val := m.Registers[m.Index[location]]
	hMux.Unlock()
	return val
}

// hash puts an object into an int64 hash.
func hash(key, value string) int64 {
	hash := siphash.New(sipConst)
	hash.Write([]byte(key + ":::" + value))
	return int64(hash.Sum64())
}

// New makes a new map register, it's not exciting.
func New(resolution int) HMSketch {
	var x HMSketch
	x.Resolution = resolution
	x.Registers = make([]hist.Histogram, 0)
	x.Index = make(map[int64]int)
	x = x.insert(map[string]string{"__global__": "__global__"}, 0, 0)
	return x
}

// Insert puts a value into a Register
func (m HMSketch) Insert(kvs map[string]string, value, count float64) HMSketch {
	m = m.insert(map[string]string{"__global__": "__global__"}, value, count)
	m = m.insert(kvs, value, count)
	return m
}

// insert does the raw  register manipulation
func (m HMSketch) insert(kvs map[string]string, value, count float64) HMSketch {
	hMux.Lock()
	for key, val := range kvs {
		location := hash(key, val)
		if _, ok := m.Index[location]; !ok {
			m.Index[location] = m.Max
			m.Max++
			m.Registers = append(m.Registers, hist.New(m.Resolution))
		}
		m.Registers[m.Index[location]] = m.Registers[m.Index[location]].Insert(value, count)
	}
	hMux.Unlock()
	return m
}

// Combine puts registers together into a register
func (m HMSketch) Combine(o HMSketch) HMSketch {
	out := New(m.Resolution)
	hMux.Lock()
	allkvps := make(map[int64]bool)
	for x := range m.Index {
		allkvps[x] = true
	}
	for x := range o.Index {
		allkvps[x] = true
	}
	for x := range allkvps {
		m1, okm := m.Index[x]
		o1, oko := o.Index[x]
		out.Index[x] = out.Max
		out.Max++
		if !okm {
			out.Registers = append(out.Registers, o.Registers[o1])
		} else if !oko {
			out.Registers = append(out.Registers, m.Registers[m1])
		} else {
			out.Registers = append(out.Registers,
				m.Registers[m1].Combine(o.Registers[o1]))
		}
	}
	hMux.Unlock()
	return out
}

// Cancel cancels histograms within the register.
func (m HMSketch) Cancel(o HMSketch) HMSketch {
	out := New(m.Resolution)
	hMux.Lock()
	allkvps := make(map[int64]bool)
	for x := range m.Index {
		allkvps[x] = true
	}
	for x := range o.Index {
		allkvps[x] = true
	}
	for x := range allkvps {
		m1, okm := m.Index[x]
		o1, oko := o.Index[x]
		out.Index[x] = out.Max
		out.Max++
		if !okm {
			out.Registers = append(out.Registers,
				hist.New(m.Resolution).Cancel(o.Registers[o1]))
		} else if !oko {
			out.Registers = append(out.Registers, m.Registers[m1])
		} else {
			out.Registers = append(out.Registers,
				m.Registers[m1].Cancel(o.Registers[o1]))
		}
	}
	hMux.Unlock()
	return out
}

// Sketch gets the values out of a histogram
func (m HMSketch) Sketch(kvs map[string]string) hist.Histogram {
	output := m.global()
	hMux.Lock()
	for key, val := range kvs {
		location := hash(key, val)
		if _, ok := m.Index[location]; !ok {
			hMux.Unlock()
			return hist.New(m.Resolution)
		}
		output = output.Min(m.Registers[m.Index[location]])
	}
	hMux.Unlock()
	return output
}

// Count returns the count of all items at a location and time
func (h HMSketch) Count(kvs map[string]string) float64 {
	hist := h.Sketch(kvs)
	return hist.Total()
}

// TotalCount returns the count of all locations at all times
func (h HMSketch) TotalCount() float64 {
	return h.global().Total()
}

// Serialize will render the HMSketch as bytes.
func (h HMSketch) Serialize() ([]byte, error) {
	var outbytes bytes.Buffer
	enc := gob.NewEncoder(&outbytes)
	err := enc.Encode(h)
	if err != nil {
		return nil, err
	}
	return outbytes.Bytes(), nil
}

// Deserialize is the inverse of serialize.
func Deserialize(input []byte) (HMSketch, error) {
	var inbytes bytes.Buffer
	var h HMSketch
	inbytes.Write(input)
	dec := gob.NewDecoder(&inbytes)
	err := dec.Decode(&h)
	return h, err
}
