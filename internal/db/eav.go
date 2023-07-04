package db

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/meow-io/go-slick/clock"
)

const NullFlag = 1

type EAVValue struct {
	Time uint64
	Flag uint8
	Val  []byte
}

type CallbackFunc func(viewName string, phase int, groupID, id []byte) error

func EAVPack(nameIdx uint32, ts uint64, null bool, val []byte) []byte {
	ret := make([]byte, 0, 13+len(val))
	ret = binary.BigEndian.AppendUint32(ret, nameIdx)
	ret = binary.BigEndian.AppendUint64(ret, ts)
	if null {
		ret = append(ret, uint8(NullFlag))
	} else {
		ret = append(ret, uint8(0))
	}
	ret = append(ret, val...)
	return ret
}

func EAVExtractNameValues(in []byte) (map[uint32]*EAVValue, error) {
	version := in[0]
	if version != 0 {
		return nil, fmt.Errorf("expected version 0, got %d", version)
	}
	c := binary.BigEndian.Uint16(in[1:])
	headerLen := uint32(19 + c*9)
	ret := make(map[uint32]*EAVValue, c)
	pos := uint32(19)
	for i := uint16(0); i != c; i++ {
		valuePos := binary.BigEndian.Uint32(in[pos+4:])
		nameIdx := binary.BigEndian.Uint32(in[pos:])
		v := &EAVValue{
			Time: binary.BigEndian.Uint64(in[headerLen+valuePos+4:]),
			Flag: in[pos+8],
			Val:  nil,
		}
		if v.Flag&NullFlag == 0 {
			valueLen := binary.BigEndian.Uint32(in[headerLen+valuePos:])
			v.Val = in[headerLen+valuePos+12 : headerLen+valuePos+12+valueLen]
		}
		ret[nameIdx] = v
		pos += 9
	}
	return ret, nil
}

// structure
//
// header
// 1 2 8 8 441 441 441
// version count mtime wtime val pos flags
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

type EAV struct {
	Clock clock.Clock

	callback CallbackFunc
}

func (eav *EAV) SetCallback(r CallbackFunc) {
	eav.callback = r
}

func (eav *EAV) MakeEmptyRecord() []byte {
	rec := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	rec = binary.BigEndian.AppendUint64(rec, eav.Clock.CurrentTimeMicro())
	return rec
}

func (eav *EAV) MakeRecord(packed ...[]byte) ([]byte, error) {
	rec := eav.MakeEmptyRecord()
	rec, err := eav.eavSet(rec, packed...)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (eav *EAV) eavWtime(in []byte) (float64, error) {
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	return microToFloat(binary.BigEndian.Uint64(in[11:])), nil
}

func (eav *EAV) eavCtime(id []byte) (float64, error) {
	return microToFloat(binary.BigEndian.Uint64(id)), nil
}

func (eav *EAV) eavMtime(in []byte) (float64, error) {
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	return microToFloat(binary.BigEndian.Uint64(in[3:])), nil
}

// The targets must be sorted
func (eav *EAV) eavHas(in []byte, targets ...uint32) (int, error) {
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	c := binary.BigEndian.Uint16(in[1:])
	pos := uint32(19)
	targetIdx := 0
	for i := uint16(0); i != c; i++ {
		if targetIdx == len(targets) {
			break
		}
		nameIdx := binary.BigEndian.Uint32(in[pos:])
		if nameIdx == targets[targetIdx] {
			if in[pos+8]&NullFlag != 0 {
				return 0, nil
			}
			targetIdx++
			pos += 9
			continue
		}
		if nameIdx > targets[targetIdx] {
			return 0, nil
		}
		pos += 9
	}
	if targetIdx != len(targets) {
		return 0, nil
	}
	return 1, nil
}

func (eav *EAV) eavGet(in []byte, target uint32) (interface{}, error) {
	// read number of names
	// scroll through list till you find your name
	version := in[0]
	if version != 0 {
		return 0, fmt.Errorf("expected version 0, got %d", version)
	}
	c := binary.BigEndian.Uint16(in[1:])
	headerLen := uint32(19 + c*9)
	pos := uint32(19)
	found := false
	for i := uint16(0); i != c; i++ {
		nameIdx := binary.BigEndian.Uint32(in[pos:])
		if nameIdx == target {
			if in[pos+8]&NullFlag != 0 {
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
	valuePos := binary.BigEndian.Uint32(in[pos+4:])
	valueLen := binary.BigEndian.Uint32(in[headerLen+valuePos:])
	value := in[headerLen+valuePos+12 : headerLen+valuePos+12+valueLen]
	return value, nil
}

// triples is name idx (be uint32), time (be uin64), val (n-bytes)
func (eav *EAV) eavSet(in []byte, packed ...[]byte) ([]byte, error) {
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
	newValsMap := make(map[uint32]EAVValue, newValsLen)
	for i := 0; i != newValsLen; i++ {
		nameIdx := binary.BigEndian.Uint32(packed[i][0:])
		time := binary.BigEndian.Uint64(packed[i][4:])
		flag := packed[i][12]
		val := packed[i][13:]
		nameIndexes[i] = nameIdx
		newValsMap[nameIdx] = EAVValue{time, flag, val}
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
		flags := in[pos+8]
		if namePos != len(nameIndexes) {
			if nameIndexes[namePos] == nameIdx {
				currentLen := binary.BigEndian.Uint32(in[currentHeaderLen+valuePos:])
				currentTime := binary.BigEndian.Uint64(in[currentHeaderLen+valuePos+4:])
				newValue := newValsMap[nameIndexes[namePos]]
				if currentTime < newValue.Time {
					if newValue.Time > currentMtime {
						currentMtime = newValue.Time
					}
					newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIdx, newValue.Flag, newValue.Time, newValue.Val)
					updated = true
				} else {
					currentValue := in[currentHeaderLen+valuePos+12 : currentHeaderLen+valuePos+12+currentLen]
					newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIdx, flags, currentTime, currentValue)
				}
				count++
				namePos++
				continue
			}
		}
		for namePos != len(nameIndexes) && nameIndexes[namePos] < nameIdx {
			val := newValsMap[nameIndexes[namePos]]
			if val.Time > currentMtime {
				currentMtime = val.Time
			}
			newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIndexes[namePos], val.Flag, val.Time, val.Val)
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
		if val.Time > currentMtime {
			currentMtime = val.Time
		}
		newHeader, newValues = appendHeaderValues(newHeader, newValues, nameIndexes[namePos], val.Flag, val.Time, val.Val)
		updated = true
		count++
		namePos++
	}
	ret := make([]byte, 0, len(newHeader)+len(newValues)+19)
	ret = append(ret, 0)
	ret = binary.BigEndian.AppendUint16(ret, count)
	ret = binary.BigEndian.AppendUint64(ret, currentMtime)
	if updated {
		newWtime := eav.Clock.CurrentTimeMicro()
		ret = binary.BigEndian.AppendUint64(ret, newWtime)
	} else {
		ret = binary.BigEndian.AppendUint64(ret, currentWtime)
	}
	ret = append(ret, newHeader...)
	ret = append(ret, newValues...)
	return ret, nil
}
