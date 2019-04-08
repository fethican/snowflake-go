package snowflake

import (
	"runtime"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set"
)

func TestGenerate10Sec(t *testing.T) {
	var lastID uint64
	var maxSequence = uint64(1<<SequenceBits - 1)
	var iteration uint64

	sf := NewSnowflake(time.Time{}, 34)

	initial := timeToSnowflakeUnit(time.Now())
	current := initial

	prevSeq := uint64(0)
	prevTS := uint64(0)

	for current-initial < 1000 {
		id, _ := sf.NextID()

		p_ts, _, p_seq := DecomposeParts(id)

		if p_seq < prevSeq {
			t.Logf("Max seq: %d (ts: %d)", prevSeq, p_ts)
			prevSeq = 0
		} else {
			if p_ts != prevTS {
				t.Logf("Seq belongs to separate TS: %d vs %d", p_ts, prevTS)
				t.Logf("Prev/Max seq: %d/%d", p_seq, prevSeq)
			}
			prevSeq = p_seq
		}
		prevTS = p_ts

		if id <= lastID {
			t.Logf("Last ID: %d", lastID)
			t.Logf("New ID: %d", id)
			t.Logf("Iteration: %d", iteration)
			t.Fatal("Duplicate ID")

		}
		lastID = id

		if p_seq > maxSequence {
			t.Fatal("Max sequence has been reached!")
		}

		current = timeToSnowflakeUnit(time.Now())

		iteration++
	}
}

func TestGenerateParallel(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU()*2)
	t.Logf("number of cpu: %d\n", runtime.NumCPU())

	sf := NewSnowflake(time.Time{}, 34)

	consumer := make(chan uint64)

	const numID = 10000
	generate := func() {
		for i := 0; i < numID; i++ {
			id, _ := sf.NextID()
			consumer <- id
		}
	}

	const numGenerator = 10
	for i := 0; i < numGenerator; i++ {
		go generate()
	}

	set := mapset.NewSet()
	for i := 0; i < numID*numGenerator; i++ {
		id := <-consumer
		if set.Contains(id) {
			t.Fatal("duplicated id")
		} else {
			set.Add(id)
		}
	}
	t.Logf("number of id: %d\n", set.Cardinality())
}

func TestEpochOverflow(t *testing.T) {
	today := time.Now()

	year := time.Duration(365*24) * time.Hour
	year138 := today.Add(-(year*138))

	sf := NewSnowflake(year138, 137)

	_, err := sf.NextID()

	if err != nil {
		t.Error("Should be in custom epoch range")
	}

	// Overflow allowed range by a second
	year140 := today.Add(-(1<<EpochBits)*snowflakeTimeUnit - time.Second)

	sf2 := NewSnowflake(year140, 137)

	_, err = sf2.NextID()

	if err == nil {
		t.Error("should return maximum timestamp error")
	}
}

func BenchmarkSnowflake(b *testing.B) {
	sf := NewSnowflake(time.Now(), 34)

	for n := 0; n < b.N; n++ {
		sf.NextID()
	}
}
