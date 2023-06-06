package bencode

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

type sortedValues []reflect.Value

func (s sortedValues) Len() int      { return len(s) }
func (s sortedValues) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedValues) Less(i, j int) bool {
	switch s[i].Type().Kind() {
	case reflect.Array:
		switch s[i].Type().Elem().Kind() {
		case reflect.Uint8:
			l := s[i].Len()
			for x := 0; x != l; x++ {
				ei := s[i].Index(x).Uint()
				ej := s[j].Index(x).Uint()
				if ei < ej {
					return true
				} else if ei > ej {
					return false
				}
			}
			return false
		default:
			panic(fmt.Sprintf("cannot sort a elem type of %#v", s[i].Type().Elem().Kind()))
		}
	case reflect.String:
		return s[i].String() < s[j].String()
	case reflect.Uint64:
		return s[i].Uint() < s[j].Uint()
	default:
		panic(fmt.Sprintf("cannot sort a type of %#v", s[i].Type().Kind()))
	}
}

// Compare two structs in shortlex-order based on their bencode-encoding.
// Return 0 for equal, -1 for a is less than b, and 1 for b is greater than a.
func Compare(a interface{}, b interface{}) (int, error) {
	abytes, err := Serialize(a)
	if err != nil {
		return 0, err
	}
	bbytes, err := Serialize(b)
	if err != nil {
		return 0, err
	}
	if len(abytes) < len(bbytes) {
		return -1, err
	} else if len(abytes) > len(bbytes) {
		return 1, err
	} else {
		return bytes.Compare(abytes, bbytes), nil
	}
}

// Serialize a ptr to a bencode-encoded byte-slice.
func Serialize(s interface{}) ([]byte, error) {
	w := newWriter()
	val := reflect.ValueOf(s)
	if val.Type().Kind() != reflect.Ptr {
		return nil, fmt.Errorf("this is not pointer")
	}
	if err := w.writeValue(val.Elem()); err != nil {
		return nil, err
	}
	return w.buf.Bytes(), nil
}

type writer struct {
	buf bytes.Buffer
}

func newWriter() writer {
	return writer{}
}

func (w *writer) writeByte(b byte) error {
	return w.buf.WriteByte(b)
}

func (w *writer) writeBytes(b []byte) error {
	if _, err := w.buf.WriteString(strconv.Itoa(len(b))); err != nil {
		return err
	}
	if err := w.buf.WriteByte(bytesLengthSep); err != nil {
		return err
	}
	if _, err := w.buf.Write(b); err != nil {
		return err
	}
	return nil
}

func (w *writer) writeSignedNumber(n int64) error {
	if err := w.buf.WriteByte(numberStart); err != nil {
		return err
	}
	if _, err := w.buf.WriteString(strconv.FormatInt(n, 10)); err != nil {
		return err
	}
	return w.writeByte(bencodeEnd)
}

func (w *writer) writeUnsignedNumber(n uint64) error {
	if err := w.buf.WriteByte(numberStart); err != nil {
		return err
	}
	if _, err := w.buf.WriteString(strconv.FormatUint(n, 10)); err != nil {
		return err
	}
	return w.writeByte(bencodeEnd)
}

func (w *writer) writeValue(v reflect.Value) error {
	switch v.Type().Kind() {
	case reflect.Bool:
		if v.Bool() {
			return w.writeUnsignedNumber(1)
		}
		return w.writeUnsignedNumber(0)
	case reflect.Int64:
		return w.writeSignedNumber(v.Int())
	case reflect.Int8:
		return w.writeSignedNumber(v.Int())
	case reflect.Uint64:
		return w.writeUnsignedNumber(v.Uint())
	case reflect.Uint32:
		return w.writeUnsignedNumber(v.Uint())
	case reflect.Uint8:
		return w.writeUnsignedNumber(v.Uint())
	case reflect.Array:
		switch v.Type().Elem().Kind() {
		case reflect.Uint8:
			b := make([]uint8, v.Len())
			reflect.Copy(reflect.ValueOf(b), v)
			return w.writeBytes(b[:])
		default:
			if err := w.writeByte(listStart); err != nil {
				return err
			}
			for i := 0; i != v.Len(); i++ {
				if err := w.writeValue(v.Index(i)); err != nil {
					return err
				}
			}
			if err := w.writeByte(bencodeEnd); err != nil {
				return err
			}
			return nil
		}
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		case reflect.Uint8:
			b := make([]uint8, v.Len())
			reflect.Copy(reflect.ValueOf(b), v)
			return w.writeBytes(b[:])
		default:
			if err := w.writeByte(listStart); err != nil {
				return err
			}
			for i := 0; i != v.Len(); i++ {
				if err := w.writeValue(v.Index(i)); err != nil {
					return err
				}
			}
			if err := w.writeByte(bencodeEnd); err != nil {
				return err
			}
			return nil
		}
	case reflect.String:
		return w.writeBytes([]byte(v.String()))
	case reflect.Struct:
		return w.writeStruct(v.Interface())
	case reflect.Map:
		if err := w.writeByte(dictStart); err != nil {
			return err
		}
		keys := v.MapKeys()
		sort.Sort(sortedValues(keys))
		for _, k := range keys {
			if err := w.writeValue(k); err != nil {
				return err
			}
			val := v.MapIndex(k)
			if err := w.writeValue(val); err != nil {
				return err
			}
		}
		if err := w.writeByte(bencodeEnd); err != nil {
			return err
		}
		return nil
	case reflect.Pointer:
		return w.writeValue(reflect.Indirect(v))
	default:
		return fmt.Errorf("unrecognized value type %#v %s", v, v.Type().Kind().String())
	}
}

func (w *writer) writeStruct(o interface{}) error {
	if err := w.writeByte(dictStart); err != nil {
		return err
	}

	ty := reflect.ValueOf(o).Type()
	fields := make(map[string]reflect.StructField)
	names := make([]string, 0, ty.NumField())
	for i := 0; i != ty.NumField(); i++ {
		f := ty.Field(i)
		if !f.IsExported() {
			continue
		}
		t := f.Tag.Get("bencode")
		if t == "" {
			return errors.New("expected bencode tag")
		}
		fields[t] = f
		names = append(names, t)
	}
	sort.Strings(names)
	structValue := reflect.ValueOf(o)
	for _, name := range names {
		structField, ok := fields[name]
		if !structField.IsExported() {
			continue
		}
		if !ok {
			return fmt.Errorf("unable to find field for %s", name)
		}
		field := structValue.FieldByName(structField.Name)
		if err := w.writeBytes([]byte(name)); err != nil {
			return err
		}
		if err := w.writeValue(field); err != nil {
			return err
		}
	}
	return w.writeByte(bencodeEnd)
}
