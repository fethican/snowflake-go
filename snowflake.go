package snowflake

/*

id is composed of:
  + time - 42 bits (millisecond precision w/ a custom epoch gives us 139 years)
  + configured machine id - 10 bits - gives us up to 1024 machines
  + sequence number - 12 bits - rolls over every 4096 per machine (with protection to avoid rollover in the same ms)

You should use NTP to keep your system clock accurate

 */

import (
	"errors"
	"sync"
	"time"
)

const (
	TotalBits = 64
	EpochBits = 42  // 139 years with custom epoch in milliseconds
	MachineIDBits = 10  // up to 1024 nodes
	SequenceBits = 12  // up to 4096 unique ids for the same timestamp

	maxNodeID = int(1<<MachineIDBits - 1)
)

type Snowflake struct {
	StartTime int64
	MachineID uint64
	Sequence uint16

	lastTimestamp int64

	mutex *sync.Mutex
}

const snowflakeTimeUnit = 1e6 // nsec, i.e. 1 msec

var epochStart = time.Date(2019, 4, 1, 0, 0, 0, 0, time.UTC)


func NewSnowflake(starttime time.Time, machineID int) *Snowflake {
	sf := new(Snowflake)
	sf.mutex = new(sync.Mutex)

	if starttime.After(time.Now()) {
		// Cannot be later than now
		return nil
	}

	if starttime.IsZero() {
		sf.StartTime = timeToSnowflakeUnit(epochStart)
	} else {
		sf.StartTime = timeToSnowflakeUnit(starttime)

		// RM ME
		epochStart = starttime
	}

	sf.MachineID = uint64(machineID & maxNodeID)

	return sf
}

func (sf *Snowflake)NextID() (uint64, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	currentTimestamp := elapsedTime(sf.StartTime)

	if sf.lastTimestamp < currentTimestamp {
		sf.lastTimestamp = currentTimestamp
		sf.Sequence = 0
	} else {
		sf.Sequence = (sf.Sequence + 1) & uint16(1<<SequenceBits - 1)
		if sf.Sequence == 0 {
			sf.lastTimestamp++

			// Adjust sleep time until next snowflakeTimeUnit which is < 1msec
			standby := time.Duration(sf.lastTimestamp-currentTimestamp) * snowflakeTimeUnit - time.Duration(time.Now().UTC().UnixNano()%snowflakeTimeUnit)*time.Nanosecond
			time.Sleep(standby)
		}
	}

	if sf.Sequence > (1<<SequenceBits-1) {
		panic("Max sequence has been reached")
	}

	if sf.lastTimestamp >= 1<<EpochBits {
		return 0, errors.New("maximum timestamp has been reached")
	}

	var id uint64

	id = uint64(sf.lastTimestamp) << (MachineIDBits + SequenceBits)
	id |= sf.MachineID << SequenceBits
	id |= uint64(sf.Sequence)

	return id, nil
}

func timeToSnowflakeUnit(t time.Time) int64 {
	return t.UTC().UnixNano() / snowflakeTimeUnit
}

func (sf Snowflake) snowflakeUnitToTime(t int64) time.Time {
	return time.Unix(0, (sf.StartTime*snowflakeTimeUnit)+(t*snowflakeTimeUnit))
}

func elapsedTime(startTime int64) int64 {
	return timeToSnowflakeUnit(time.Now()) - startTime
}

func DecomposeParts(id uint64) (uint64, uint64, uint64){
	const maskMachineID = uint64(1<<MachineIDBits - 1) << SequenceBits
	const maskSequence = uint64(1<<SequenceBits - 1)

	t := id >> ( MachineIDBits + SequenceBits)
	mid := id & maskMachineID >> SequenceBits
	seq := id & maskSequence

	return t, mid, seq
}

/*
func (sf Snowflake) ToString() string {
	fromElapsedToTime(sf.StartTime)
	return fmt.Sprintf("MachineID: %d\nSequence: %d\n", sf.MachineID, sf.Sequence)
}*/
/*
func fromElapsedToTime(elapsed int64)  {
	t := time.Unix(0, elapsed*snowflakeTimeUnit)
	fmt.Printf("Time: %s (%d)\n", t.UTC(), t.UnixNano())
}*/

/*
func Decompose(id uint64, starttime time.Time) {
	t, mid, seq := DecomposeParts(id)

	if !starttime.IsZero() {
		t += uint64(timeToSnowflakeUnit(starttime)) + t
	}

	t2 := time.Unix(0, epochStart.UTC().UnixNano()+ int64(t)*snowflakeTimeUnit)

	fmt.Printf(
		"time: %s (%d)\nmachine id: %d\nseq: %d\n",
		t2.UTC(),
		t2.UnixNano(),
		mid,
		seq,
		)
}*/