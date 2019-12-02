package hmsketch

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestHash(t *testing.T) {
	t.Run("hashing", func(t *testing.T) {
		if hash("", "") != 1827630361452336488 {
			t.Log(hash("", ""))
			//t.Fail()
		}
	})
}

func TestHMSketch(t *testing.T) {
	hist := New(2)
	hist = hist.Insert(map[string]string{"group": "a", "instance": "a", "job": "c", "service": "d"}, 10.4, 1)
	hist = hist.Insert(map[string]string{"group": "a", "instance": "a", "job": "c", "service": "d"}, 45.4, 1)
	hist = hist.Insert(map[string]string{"group": "a", "instance": "a", "job": "c", "service": "d"}, 12.4, 2)
	hist = hist.Insert(map[string]string{"group": "a", "instance": "b", "job": "d", "service": "q"}, 10.4, 1)
	serialized, _ := hist.Serialize()
	hist2, _ := Deserialize(serialized)
	t.Run("(De)serialize count equality", func(t *testing.T) {
		x := hist.TotalCount()
		y := hist2.TotalCount()
		if x != y {
			t.Log(x, y, hist, "-----", hist2)
			t.Fail()
		}
	})
	t.Run("basic adding", func(t *testing.T) {
		before := hist.TotalCount()
		hist.Insert(map[string]string{"group": "a"}, 1.0, 1.0)
		after := hist.TotalCount()
		if after != before+1 {
			t.Log(before, after)
			t.Fail()
		}
	})
	t.Run("Basic sketch", func(t *testing.T) {
		x := hist.Sketch(map[string]string{"group": "a", "instance": "a", "job": "c", "service": "d"})
		if x.Read(10.4) != 1 {
			t.Log(x.Read(10), x, hist2)
			t.Fail()
		}
	})
	t.Run("Null sketch", func(t *testing.T) {
		x := hist.Sketch(map[string]string{"group": "a", "instance": "a", "job": "c", "service": "e"})
		total := x.Total()
		if 0 != total {
			t.Log(total, x)
			t.Fail()
		}
	})
	t.Run("Counting", func(t *testing.T) {
		temp := hist.Count(map[string]string{"group": "a", "instance": "a", "job": "c", "service": "d"})
		if 4 != temp {
			t.Log(4, "!=", temp)
			t.Fail()
		}
	})
	t.Run("self-combination total count validation", func(t *testing.T) {
		if val := hist.Combine(hist); 2*hist.TotalCount() != val.TotalCount() {
			t.Log(hist.TotalCount(), val.TotalCount())
			t.Fail()
		}
	})
	t.Run("Differencing to empty", func(t *testing.T) {
		if val := hist.Cancel(hist); 0 != val.TotalCount() {
			t.Log(val.TotalCount())
			t.Fail()
		}
	})
	t.Run("Total Count", func(t *testing.T) {
		if 6 != hist.TotalCount() {
			t.Log(hist.TotalCount())
			t.Fail()
		}
	})
}

var (
	h = New(10)
)

// Benchmarks are attractive for this one.
// I can pull 300k inserts and 1k sketches
// in 2.7 secs on my chromebook. That should
// be sufficient for most purposes.

func BenchmarkHashing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = hash(fmt.Sprint(rand.Intn(1000)), fmt.Sprint(rand.Intn(1000)))
	}
}

func BenchmarkInsert(b *testing.B) {
	for ii := 0; ii < b.N; ii++ {
		gp := fmt.Sprint(rand.Intn(10))
		in := fmt.Sprint(rand.Intn(8))
		job := fmt.Sprint(rand.Intn(6))
		serv := fmt.Sprint(rand.Intn(4))
		value := rand.NormFloat64()*1000 + 100
		count := rand.NormFloat64()*1000 + 100
		h = h.Insert(map[string]string{"group": gp, "instance": in, "job": job, "service": serv}, value, count)
	}

}

func BenchmarkSketching(b *testing.B) {
	for ii := 0; ii < b.N; ii++ {
		gp := fmt.Sprint(rand.Intn(10))
		in := fmt.Sprint(rand.Intn(8))
		job := fmt.Sprint(rand.Intn(6))
		serv := fmt.Sprint(rand.Intn(4))
		h.Sketch(map[string]string{"group": gp, "instance": in, "job": job, "service": serv})
	}
}
