// A thin wrapper over the system clock which can be implemented for use in tests.
package clock

import "time"

type Clock interface {
	CurrentTimeMicro() uint64
	CurrentTimeMs() uint64
	CurrentTimeSec() uint64
	Now() time.Time
}

type systemClock struct{}

func NewSystemClock() Clock {
	return &systemClock{}
}

func (sc *systemClock) CurrentTimeMicro() uint64 {
	return uint64(time.Now().UnixMicro())
}

func (sc *systemClock) CurrentTimeMs() uint64 {
	return sc.CurrentTimeMicro() / 1000
}

func (sc *systemClock) CurrentTimeSec() uint64 {
	return sc.CurrentTimeMicro() / 1000000
}

func (sc *systemClock) Now() time.Time {
	return time.Now()
}
