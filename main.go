package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/bits"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"syscall"
	"unsafe"
)

type measurement struct {
	min   int64
	max   int64
	sum   int64
	count int64
}

func main() {
	if len(os.Args) != 2 && len(os.Args) != 3 {
		log.Fatalf("Usage: %s <measurements> [profile]", os.Args[0])
	}

	if len(os.Args) == 3 {
		prof, err := os.Create(os.Args[2])
		if err != nil {
			panic(err)
		}
		if err = pprof.StartCPUProfile(prof); err != nil {
			panic(err)
		}
		defer func() {
			pprof.StopCPUProfile()
		}()
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := fi.Size()
	if size <= 0 {
		panic("invalid file size")
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	chunks := split(data, runtime.NumCPU())
	results := make(chan *robinHoodMap, len(chunks))
	for i, chunk := range chunks {
		go func(i int, chunk []byte) {
			results <- process(chunk, newRobinHoodMap(5000))
		}(i, chunk)
	}

	cities := <-results
	for i := 1; i < len(chunks); i++ {
		r := <-results
		r.Iterate(func(name string, rm *measurement) {
			cities.Upsert(name, func(m *measurement) {
				if m.count == 0 {
					*m = *rm
				} else {
					m.min = min(m.min, rm.min)
					m.max = max(m.max, rm.max)
					m.sum += rm.sum
					m.count += rm.count
				}
			})
		})
	}

	var names []string
	cities.Iterate(func(name string, _ *measurement) {
		names = append(names, name)
	})
	sort.Strings(names)

	// fmt.Print("{")
	for _, name := range names {
		// if i > 0 {
		// 	fmt.Print(", ")
		// }
		cities.Upsert(name, func(m *measurement) {
			fmt.Printf("%s=%d/%d.%d/%.1f/%d.%d\n", name, m.count,
				m.min/10, m.min%10, (float64(m.sum)/float64(m.count))/10, m.max/10, m.max%10)
		})
	}
	// fmt.Println("}")
}

func split(data []byte, n int) [][]byte {
	chunks := make([][]byte, n)
	chunkSize := len(data) / n
	var start int
	for i := range chunks {
		end := (i + 1) * chunkSize
		if end >= len(data) {
			end = len(data)
		} else if j := bytes.IndexByte(data[end:], '\n'); j == -1 {
			end = len(data)
		} else {
			end += j + 1
		}
		chunks[i] = data[start:end]
		start = end
	}
	return chunks
}

func process(data []byte, cities *robinHoodMap) *robinHoodMap {
	for i := 0; i < len(data); {
		// Assume the data is well-formed and that a semicolon must be
		// present.
		start := i
		for ; ; i++ {
			if data[i] == ';' {
				break
			}
		}
		name := unsafe.String(&data[start], i-start)
		i++

		// Assume the data is well-formed, there are 4 possibilies for
		// temperatures in the range -99.9 to 99.9: -xx.x, -x.x, x.x, xx.x
		var temp int
		if b := data[i]; b == '-' {
			if data[i+2] == '.' {
				// 1: -x.x
				temp = -10*int(data[i+1]&0xf) - int(data[i+3]&0xf)
				i += 5
			} else {
				// 2: -xx.x
				temp = -100*int(data[i+1]&0xf) - 10*int(data[i+2]&0xf) - int(data[i+4]&0xf)
				i += 6
			}
		} else if data[i+1] == '.' {
			// 3: x.x
			temp = 10*int(b&0xf) + int(data[i+2]&0xf)
			i += 4
		} else {
			// 4: xx.x
			temp = 100*int(b&0xf) + 10*int(data[i+1]&0xf) + int(data[i+3]&0xf)
			i += 5
		}

		cities.Upsert(name, func(m *measurement) {
			if m.count == 0 {
				m.min = math.MaxInt64
				m.max = math.MinInt64
			}
			m.min = min(m.min, int64(temp))
			m.max = max(m.max, int64(temp))
			m.sum += int64(temp)
			m.count++
		})
	}

	return cities
}

func robinHoodHash(k string, shift uint32) uint32 {
	hash := uint64(14695981039346656037)
	for _, c := range k {
		hash ^= uint64(c)
		hash *= 1099511628211
	}
	return uint32(hash >> shift)
}

type robinHoodEntry struct {
	key   string
	value measurement
	dist  uint32 // The distance the entry is from its desired position.
}

type robinHoodMap struct {
	entries []robinHoodEntry
	size    uint32
	shift   uint32
	maxDist uint32
}

func newRobinHoodMap(initialCapacity int) *robinHoodMap {
	m := &robinHoodMap{}
	if initialCapacity < 1 {
		initialCapacity = 1
	}
	targetSize := 1 << (uint(bits.Len(uint(2*initialCapacity-1))) - 1)
	m.rehash(uint32(targetSize))
	return m
}

func (m *robinHoodMap) rehash(size uint32) {
	oldEntries := m.entries

	m.size = size
	m.shift = uint32(64 - bits.Len32(m.size-1))
	m.maxDist = max(uint32(bits.Len32(size)), 4)
	m.entries = make([]robinHoodEntry, size+m.maxDist)

	for i := range oldEntries {
		if e := &oldEntries[i]; e.key != "" {
			m.Upsert(e.key, func(m *measurement) {
				*m = e.value
			})
		}
	}
}

func (m *robinHoodMap) Upsert(k string, f func(v *measurement)) {
	maybeExists := true
	n := robinHoodEntry{key: k, dist: 0}
	for i := robinHoodHash(n.key, m.shift); ; i++ {
		e := &m.entries[i]
		if maybeExists && k == e.key {
			// Entry already exists: overwrite.
			f(&e.value)
			return
		}

		if e.key == "" {
			// Found an empty entry: insert here.
			*e = n
			if maybeExists {
				f(&e.value)
			}
			return
		}

		if e.dist < n.dist {
			// Swap the new entry with the current entry because the current is
			// rich. We then continue to loop, looking for a new location for the
			// current entry. Note that this is also the not-found condition for
			// retrieval, which means that "k" is not present in the map. See Get().
			n, *e = *e, n
			if maybeExists {
				f(&e.value)
				maybeExists = false
			}
		}

		// The new entry gradually moves away from its ideal position.
		n.dist++

		// If we've reached the max distance threshold, grow the table and restart
		// the insertion.
		if n.dist == m.maxDist {
			m.rehash(2 * m.size)
			n.dist = 0
			i = robinHoodHash(n.key, m.shift) - 1
		}
	}
}

func (m *robinHoodMap) Iterate(f func(k string, m *measurement)) {
	for i := range m.entries {
		if e := &m.entries[i]; e.key != "" {
			f(e.key, &e.value)
		}
	}
}
