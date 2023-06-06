package slick

import (
	"fmt"

	"github.com/meow-io/go-slick/data/eav"
	"github.com/meow-io/go-slick/ids"
)

func addOps(slick *Slick, tablename string, id ids.ID, ts uint64, values map[string]interface{}, ops *eav.Operations) error {
	s, ok := slick.data.EAV.Schema(tablename)
	if !ok {
		return fmt.Errorf("cannot find table %s", tablename)
	}
	for name := range values {
		val := values[name]

		c, ok := s.Columns[name]
		if !ok {
			return fmt.Errorf("no column for %s in %s", name, tablename)
		}

		if val == nil {
			if !c.Nullable {
				return fmt.Errorf("cannot have a nil value for %s in %s", name, tablename)
			}
			ops.AddNil(id, ts, c.SourceName)
			continue
		}

		switch t := val.(type) {
		case string:
			if c.ColumnType != eav.Text {
				return fmt.Errorf("column is not text for %s in %s", name, tablename)
			}
			ops.AddString(id, ts, c.SourceName, t)
		case uint64:
			if c.ColumnType != eav.Int {
				return fmt.Errorf("column is not int for %s in %s", name, tablename)
			}
			ops.AddInt64(id, ts, c.SourceName, int64(t))
		case int64:
			if c.ColumnType != eav.Int {
				return fmt.Errorf("column is not int for %s in %s", name, tablename)
			}
			ops.AddInt64(id, ts, c.SourceName, t)
		case int:
			if c.ColumnType != eav.Int {
				return fmt.Errorf("column is not int for %s in %s", name, tablename)
			}
			ops.AddInt64(id, ts, c.SourceName, int64(t))
		case bool:
			if c.ColumnType != eav.Int {
				return fmt.Errorf("column is not int for %s in %s", name, tablename)
			}
			if t {
				ops.AddInt64(id, ts, c.SourceName, 1)
			} else {
				ops.AddInt64(id, ts, c.SourceName, 0)
			}
		case float64:
			if c.ColumnType != eav.Real {
				return fmt.Errorf("column is not real for %s in %s", name, tablename)
			}
			ops.AddFloat64(id, ts, c.SourceName, t)
		case []byte:
			if c.ColumnType != eav.Blob {
				return fmt.Errorf("column is not blob for %s in %s", name, tablename)
			}
			ops.AddBytes(id, ts, c.SourceName, t)
		default:
			return fmt.Errorf("unrecognized type %T", val)
		}
	}

	return nil
}

type op interface {
	execute(ts uint64, ops *eav.Operations) (ids.ID, error)
}

type insertOp struct {
	writer    *EAVWriter
	tablename string
	values    map[string]interface{}
}

func (o *insertOp) execute(ts uint64, ops *eav.Operations) (ids.ID, error) {
	id, err := o.writer.slick.NewID(o.writer.authorTag)
	if err != nil {
		return id, err
	}

	return id, addOps(o.writer.slick, o.tablename, id, ts, o.values, ops)
}

type updateOp struct {
	writer    *EAVWriter
	tablename string
	id        []byte
	values    map[string]interface{}
}

func (o *updateOp) execute(ts uint64, ops *eav.Operations) (ids.ID, error) {
	return ids.IDFromBytes(o.id), addOps(o.writer.slick, o.tablename, ids.IDFromBytes(o.id), ts, o.values, ops)
}

type EAVWriter struct {
	slick     *Slick
	groupID   ids.ID
	authorTag [7]byte
	ops       []op
	InsertIDs []ids.ID
}

func (w *EAVWriter) Insert(tablename string, values map[string]interface{}) {
	w.ops = append(w.ops, &insertOp{writer: w, tablename: tablename, values: values})
}

func (w *EAVWriter) Update(tablename string, id []byte, values map[string]interface{}) {
	w.ops = append(w.ops, &updateOp{writer: w, tablename: tablename, id: id, values: values})
}

func (w *EAVWriter) Execute() error {
	ops, err := w.newOps()
	if err != nil {
		return err
	}
	return w.slick.EAVWrite(w.groupID, ops)
}

func (w *EAVWriter) execute() error {
	ops, err := w.newOps()
	if err != nil {
		return err
	}
	return w.slick.eavWrite(w.groupID, ops)
}

func (w *EAVWriter) newOps() (*eav.Operations, error) {
	currentMicro := w.slick.clock.CurrentTimeMicro()
	ops := eav.NewOperations()

	for _, op := range w.ops {
		id, err := op.execute(currentMicro, ops)
		if err != nil {
			return nil, err
		}

		if _, ok := op.(*insertOp); ok {
			w.InsertIDs = append(w.InsertIDs, id)
		}
	}
	return ops, nil
}
