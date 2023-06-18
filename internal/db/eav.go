package db

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/meow-io/go-slick/clock"
)

const deletedFlag = 1

type eavValue struct {
	time uint64
	flag uint8
	val  []byte
}

func EAVPack(nameIdx uint32, ts uint64, null bool, val []byte) []byte {
	ret := make([]byte, 0, 13+len(val))
	ret = binary.BigEndian.AppendUint32(ret, nameIdx)
	ret = binary.BigEndian.AppendUint64(ret, ts)
	if null {
		ret = append(ret, uint8(deletedFlag))
	} else {
		ret = append(ret, uint8(0))
	}
	ret = append(ret, val...)
	return ret
}

// structure
//
// header
// 1 2 8 441 441 441
// version count mtime val pos flags
//
// values
// 4 8 n-bytes
// len time val
//
// pos is relative to after-header
// time is microseconds

func appendHeaderValues(header, values []byte, nameIdx uint32, flags uint8, ts uint64, val []byte) ([]byte, []byte) {
	header = binary.BigEndian.AppendUint32(header, nameIdx)
	header = binary.BigEndian.AppendUint32(header, uint32(len(values)))
	header = append(header, flags)
	values = binary.BigEndian.AppendUint32(values, uint32(len(val)))
	values = binary.BigEndian.AppendUint64(values, ts)
	values = append(values, val...)
	return header, values
}

func microToFloat(ts uint64) float64 {
	return float64(ts) / 1000000
}

type eav struct {
	cl clock.Clock
}

func (eav *eav) MakeEmptyRecord() []byte {
	rec := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	rec = binary.BigEndian.AppendUint64(rec, eav.cl.CurrentTimeMicro())
	return rec
}

func (eav *eav) eavWtime(id []byte) (float64, error) {
	return microToFloat(binary.BigEndian.Uint64(id[0:8])), nil
}

func (eav *eav) eavCtime(id []byte) (float64, error) {
	return microToFloat(binary.BigEndian.Uint64(id[0:8])), nil
}

func (eav *eav) eavMtime(in []byte) (float64, error) {
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	mtime := binary.BigEndian.Uint64(in[3:])
	return microToFloat(mtime), nil
}

func (eav *eav) eavHas(in []byte, targets ...uint32) (int, error) {
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	c := binary.BigEndian.Uint16(in[1:])
	pos := uint32(19)
	targetIdx := 0
	for i := uint16(0); i != c; i++ {
		nameIdx := binary.BigEndian.Uint32(in[pos:])
		pos += 9
		if nameIdx == targets[targetIdx] {
			targetIdx++
			continue
		}
		if nameIdx > targets[targetIdx] {
			return 0, nil
		}
	}
	if targetIdx != len(targets) {
		return 0, nil
	}
	return 1, nil
}

func (eav *eav) eavGet(in []byte, target uint32) (interface{}, error) {
	// read number of names
	// scroll through list till you find your name
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	c := binary.BigEndian.Uint16(in[1:])
	pos := uint32(19)
	found := false
	for i := uint16(0); i != c; i++ {
		nameIdx := binary.BigEndian.Uint32(in[pos:])
		if nameIdx == target {
			if in[pos+8]&deletedFlag != 0 {
				return nil, nil
			}
			found = true
			break
		}
		if nameIdx > target {
			return nil, nil
		}
		pos += 9
	}
	if !found {
		return nil, nil
	}
	headerLen := uint32(19 + c*9)
	valuePos := binary.BigEndian.Uint32(in[pos+4:])
	valueLen := binary.BigEndian.Uint32(in[headerLen+valuePos:])
	value := in[headerLen+valuePos+12 : headerLen+valuePos+12+valueLen]
	return value, nil
}

// triples is name idx (be uint32), time (be uin64), val (n-bytes)
func (eav *eav) eavSet(in []byte, packed ...[]byte) ([]byte, error) {
	var currentCount uint16
	var currentMtime, currentWtime uint64
	if len(in) != 0 {
		version := in[0]
		if version != 0 {
			return nil, fmt.Errorf("expected version 0, got %d", version)
		}
		currentCount = binary.BigEndian.Uint16(in[1:])
		currentMtime = binary.BigEndian.Uint64(in[3:])
		currentWtime = binary.BigEndian.Uint64(in[11:])
	}
	currentHeaderLen := uint32(19 + currentCount*9)
	// assemble vals
	newValsLen := len(packed)
	nameIndexes := make([]uint32, newValsLen)
	newValsMap := make(map[uint32]eavValue, newValsLen)
	for i := 0; i != newValsLen; i++ {
		nameIdx := binary.BigEndian.Uint32(packed[i][0:])
		time := binary.BigEndian.Uint64(packed[i][4:])
		flag := packed[i][12]
		val := packed[i][13:]
		nameIndexes[i] = nameIdx
		newValsMap[nameIdx] = eavValue{time, flag, val}
	}

	sort.Slice(nameIndexes, func(i, j int) bool { return nameIndexes[i] < nameIndexes[j] })
	// construct new header
	count := uint16(0)
	updated := false
	namePos := 0
	newHeader := make([]byte, 0, currentCount+uint16((newValsLen)*9))
	var newValues []byte
	if len(in) != 0 {
		newValues = make([]byte, 0, (len(in)-2-int(currentCount)*8)*2)
	}
	for i := uint16(0); i != currentCount; i++ {
		pos := i*9 + 19
		nameIdx := binary.BigEndian.Uint32(in[pos:])
		valuePos := binary.BigEndian.Uint32(in[pos+4:])
		flags := in[8]
		if nameIndexes[namePos] == nameIdx {
			currentLen := binary.BigEndian.Uint32(in[currentHeaderLen+valuePos:])
			currentTime := binary.BigEndian.Uint64(in[currentHeaderLen+valuePos+4:])
			newValue := newValsMap[nameIndexes[namePos]]
			if currentTime < newValue.time {
				if newValue.time > currentMtime {
					currentMtime = newValue.time
				}
				newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIdx, newValue.flag, newValue.time, newValue.val)
				updated = true
			} else {
				currentValue := in[currentHeaderLen+valuePos+12 : currentHeaderLen+valuePos+12+currentLen]
				newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIdx, flags, currentTime, currentValue)
			}
			count++
			namePos++
			continue
		}

		for nameIndexes[namePos] < nameIdx {
			val := newValsMap[nameIndexes[namePos]]
			if val.time > currentMtime {
				currentMtime = val.time
			}
			newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIndexes[namePos], val.flag, val.time, val.val)
			updated = true
			count++
			namePos++
		}

		currentLen := binary.BigEndian.Uint32(in[currentHeaderLen+valuePos:])
		currentTime := binary.BigEndian.Uint64(in[currentHeaderLen+valuePos+4:])
		currentValue := in[currentHeaderLen+valuePos+12 : currentHeaderLen+valuePos+12+currentLen]
		newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIdx, flags, currentTime, currentValue)
		count++
	}

	for namePos != len(nameIndexes) {
		val := newValsMap[nameIndexes[namePos]]
		if val.time > currentMtime {
			currentMtime = val.time
		}
		newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIndexes[namePos], val.flag, val.time, val.val)
		updated = true
		count++
		namePos++
	}
	ret := make([]byte, 0, len(newHeader)+len(newValues)+19)
	ret = append(ret, 0)
	ret = binary.BigEndian.AppendUint16(ret, count)
	ret = binary.BigEndian.AppendUint64(ret, currentMtime)
	if updated {
		newWtime := eav.cl.CurrentTimeMicro()
		ret = binary.BigEndian.AppendUint64(ret, newWtime)
	} else {
		ret = binary.BigEndian.AppendUint64(ret, currentWtime)
	}
	ret = append(ret, newHeader...)
	ret = append(ret, newValues...)
	return ret, nil
}
