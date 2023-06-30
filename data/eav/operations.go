package eav

import (
	"strconv"

	"github.com/meow-io/go-slick/bencode"
	"github.com/meow-io/go-slick/ids"
)

type Operations struct {
	Names        []string                               `bencode:"n"`
	OperationMap map[uint64]map[ids.ID]map[uint32]Value `bencode:"m"`

	fromBackfill bool
	nameMap      map[string]uint32
}

func NewOperations() *Operations {
	return &Operations{
		Names:        make([]string, 0),
		OperationMap: make(map[uint64]map[ids.ID]map[uint32]Value),
		nameMap:      make(map[string]uint32),
	}
}

func (o *Operations) Empty() bool {
	return len(o.OperationMap) == 0
}

func (o *Operations) AddNil(id ids.ID, ts uint64, name string) *Operations {
	return o.Add(id, ts, name, Value{false, nil})
}

func (o *Operations) AddBytes(id ids.ID, ts uint64, name string, val []byte) *Operations {
	return o.Add(id, ts, name, Value{true, val})
}

func (o *Operations) AddString(id ids.ID, ts uint64, name string, val string) *Operations {
	return o.Add(id, ts, name, Value{true, []byte(val)})
}

func (o *Operations) AddInt64(id ids.ID, ts uint64, name string, val int64) *Operations {
	return o.Add(id, ts, name, Value{true, []byte(strconv.FormatInt(val, 10))})
}

func (o *Operations) AddFloat64(id ids.ID, ts uint64, name string, val float64) *Operations {
	return o.Add(id, ts, name, Value{true, []byte(strconv.FormatFloat(val, 'f', -1, 64))})
}

func (o *Operations) Add(id ids.ID, ts uint64, name string, val Value) *Operations {
	if _, ok := o.OperationMap[ts]; !ok {
		o.OperationMap[ts] = make(map[ids.ID]map[uint32]Value)
	}
	if _, ok := o.OperationMap[ts][id]; !ok {
		o.OperationMap[ts][id] = make(map[uint32]Value)
	}
	if _, ok := o.nameMap[name]; !ok {
		nameIdx := len(o.Names)
		o.nameMap[name] = uint32(nameIdx)
		o.Names = append(o.Names, name)
	}
	o.OperationMap[ts][id][o.nameMap[name]] = val
	return o
}

func (o *Operations) AddMap(ts uint64, ops map[ids.ID]map[string]Value) *Operations {
	for id, idMap := range ops {
		for name, val := range idMap {
			o.Add(id, ts, name, val)
		}
	}
	return o
}

func DeserializeOps(b []byte) (*Operations, error) {
	o := &Operations{}
	if err := bencode.Deserialize(b, o); err != nil {
		return nil, err
	}
	return o, nil
}

func SerializeOps(ops *Operations) ([]byte, error) {
	return bencode.Serialize(ops)
}
