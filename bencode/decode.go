package bencode

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
)

type DecodeError struct {
	msg string
}

func newDecodeError(msg string, vars ...interface{}) *DecodeError {
	return &DecodeError{fmt.Sprintf(msg, vars...)}
}

func (e *DecodeError) Error() string {
	return e.msg
}

// Given the target interface, decode the following byte slice to it.
func Deserialize(buf []byte, t interface{}) error {
	r := newReader(buf)

	val := reflect.ValueOf(t)
	out, err := r.readValue(val.Type())
	if err != nil {
		return err
	}
	if val.CanAddr() {
		val.Elem().Set(out.Elem())
	} else {
		val.Elem().Set(reflect.Indirect(*out))
	}
	if !r.isAtEnd() {
		return newDecodeError("expected to be at end of buffer")
	}
	return nil
}

type reader struct {
	buf []byte
	pos int64
}

func newReader(buf []byte) reader {
	return reader{
		buf: buf,
		pos: 0,
	}
}

func (r *reader) expectByte(b byte) error {
	if int64(len(r.buf)) == r.pos {
		return newDecodeError("expected 0x%x at pos %d, but no more bytes left", b, r.pos)
	}
	c := r.buf[r.pos]
	if c != b {
		return newDecodeError("expected 0x%x got 0x%x at pos %d", b, c, r.pos)
	}
	r.pos++
	return nil
}

func (r *reader) readInt() (int64, error) {
	neg := false
	if err := r.expectByte(0x69); err != nil {
		return 0, err
	}
	if r.buf[r.pos] == 0x2d {
		neg = true
		r.pos++
	}
	l := int64(0)
	for c := r.buf[r.pos]; c >= 0x30 && c <= 0x39; c = r.buf[r.pos+l] {
		l++
	}
	if l == 0 {
		return 0, newDecodeError("expected numbers")
	}
	val, err := strconv.ParseInt(string(r.buf[r.pos:r.pos+l]), 10, 64)
	if err != nil {
		return 0, err
	}
	if val == 0 && neg {
		return 0, newDecodeError("negative 0 not allowed")
	}
	r.pos += l
	if err := r.expectByte(bencodeEnd); err != nil {
		return 0, err
	}

	return val, nil
}

func (r *reader) readUint() (uint64, error) {
	neg := false
	if err := r.expectByte(0x69); err != nil {
		return 0, err
	}
	if r.buf[r.pos] == 0x2d {
		neg = true
		r.pos++
	}
	l := int64(0)
	for c := r.buf[r.pos]; c >= 0x30 && c <= 0x39; c = r.buf[r.pos+l] {
		l++
	}
	if l == 0 {
		return 0, newDecodeError("expected numbers")
	}
	val, err := strconv.ParseUint(string(r.buf[r.pos:r.pos+l]), 10, 64)
	if err != nil {
		return 0, err
	}
	if val == 0 && neg {
		return 0, newDecodeError("negative 0 not allowed")
	}
	r.pos += l
	if err := r.expectByte(bencodeEnd); err != nil {
		return 0, err
	}

	return val, nil
}
func (r *reader) readBytes() ([]byte, error) {
	bLen := int64(0)
	for c := r.buf[r.pos+bLen]; c >= 0x30 && c <= 0x39; c = r.buf[r.pos+bLen] {
		bLen++
	}
	if bLen == 0 {
		return nil, newDecodeError("expected 1 or more numbers %d", r.pos)
	}
	numSlice := r.buf[r.pos : r.pos+bLen]
	colon := r.buf[r.pos+bLen]
	if colon != 0x3a {
		return nil, newDecodeError("expected %x to be 0x3a", colon)
	}
	l, err := strconv.ParseInt(string(numSlice), 10, 64)
	if err != nil {
		return nil, err
	}
	b := r.buf[r.pos+bLen+1 : r.pos+bLen+1+l]
	r.pos = r.pos + bLen + 1 + l
	return b, nil
}

func (r *reader) peek() byte {
	return r.buf[r.pos]
}

func (r *reader) isAtEnd() bool {
	return r.pos >= int64(len(r.buf))
}

func (r *reader) readValue(t reflect.Type) (*reflect.Value, error) {
	switch t.Kind() {
	case reflect.Bool:
		num, err := r.readUint()
		if err != nil {
			return nil, err
		}
		if num > 1 {
			return nil, fmt.Errorf("expected number to be 0 or 1, got %d", num)
		}
		val := reflect.ValueOf(num == 1)
		return &val, nil
	case reflect.Int64:
		num, err := r.readInt()
		if err != nil {
			return nil, err
		}
		val := reflect.ValueOf(num)
		return &val, nil
	case reflect.Uint8:
		num, err := r.readUint()
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint8 {
			return nil, fmt.Errorf("expected number to be less than %d, got %d", math.MaxUint8, num)
		}
		val := reflect.ValueOf(uint8(num))
		return &val, nil
	case reflect.Uint32:
		num, err := r.readUint()
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint32 {
			return nil, fmt.Errorf("expected number to be less than %d, got %d", math.MaxUint32, num)
		}
		val := reflect.ValueOf(uint32(num))
		return &val, nil
	case reflect.Uint64:
		num, err := r.readUint()
		if err != nil {
			return nil, err
		}
		val := reflect.ValueOf(uint64(num))
		return &val, nil
	case reflect.Int8:
		num, err := r.readInt()
		if err != nil {
			return nil, err
		}
		if num < math.MinInt8 || num > math.MaxInt8 {
			return nil, fmt.Errorf("expected number to be within %d and %d, got %d", math.MinInt8, math.MaxInt8, num)
		}
		val := reflect.ValueOf(int8(num))
		return &val, nil
	case reflect.String:
		b, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		val := reflect.ValueOf(string(b))
		return &val, nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			val := reflect.ValueOf(b)
			return &val, nil
		default:
			a := reflect.MakeSlice(t, 0, 0)
			if err := r.expectByte(listStart); err != nil {
				return nil, err
			}
			for r.peek() != bencodeEnd {
				val, err := r.readValue(t.Elem())
				if err != nil {
					return nil, err
				}
				a = reflect.Append(a, *val)
			}
			if err := r.expectByte(bencodeEnd); err != nil {
				return nil, err
			}
			return &a, nil
		}
	case reflect.Array:
		switch t.Elem().Kind() {
		case reflect.Uint8:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			valPtr := reflect.New(t)
			reflect.Copy(reflect.Indirect(valPtr), reflect.ValueOf(b))
			val := reflect.Indirect(valPtr)
			return &val, nil
		default:
			a := reflect.MakeSlice(t, 0, 0)
			if err := r.expectByte(listStart); err != nil {
				return nil, err
			}
			for r.peek() != bencodeEnd {
				val, err := r.readValue(t.Elem())
				if err != nil {
					return nil, err
				}
				a = reflect.Append(a, *val)
			}
			if err := r.expectByte(bencodeEnd); err != nil {
				return nil, err
			}
			return &a, nil
		}
	case reflect.Struct:
		valPtr := reflect.New(t)
		err := r.readStruct(valPtr.Interface())
		if err != nil {
			return nil, err
		}
		val := reflect.Indirect(valPtr)
		return &val, nil
	case reflect.Map:
		if err := r.expectByte(dictStart); err != nil {
			return nil, err
		}
		keyType := t.Key()
		m := reflect.MakeMap(t)
		for r.peek() != bencodeEnd {
			keyValue, err := r.readValue(keyType)
			if err != nil {
				return nil, err
			}
			valValue, err := r.readValue(t.Elem())
			if err != nil {
				return nil, err
			}
			m.SetMapIndex(*keyValue, *valValue)
		}
		if err := r.expectByte(bencodeEnd); err != nil {
			return nil, err
		}
		return &m, nil
	case reflect.Pointer:
		out, err := r.readValue(t.Elem())
		if err != nil {
			return nil, err
		}
		v := reflect.New(t.Elem())
		v.Elem().Set(*out)
		return &v, nil

	default:
		return nil, fmt.Errorf("unhandled kind %v", t.Kind())
	}
}

func (r *reader) readStruct(o interface{}) error {
	if err := r.expectByte(dictStart); err != nil {
		return err
	}

	ty := reflect.ValueOf(o).Elem().Type()
	fields := make(map[string]reflect.StructField)
	names := make([]string, 0, ty.NumField())
	for i := 0; i != ty.NumField(); i++ {
		f := ty.Field(i)
		if !f.IsExported() {
			continue
		}
		t := f.Tag.Get("bencode")
		if t == "" {
			return newDecodeError("expected bencode tag")
		}
		fields[t] = f
		names = append(names, t)
	}
	sort.Strings(names)
	structValue := reflect.ValueOf(o).Elem()
	for _, name := range names {
		structField, ok := fields[name]
		if !ok {
			return newDecodeError("unable to find field for %s", name)
		}
		field := structValue.FieldByName(structField.Name)
		buf, err := r.readBytes()
		if err != nil {
			return err
		}
		if buf == nil {
			return newDecodeError("expected bytes")
		}
		potentialStr := string(buf)
		t := structField.Type
		if potentialStr != name {
			return newDecodeError("missing key for %s got %s instead", name, potentialStr)
		}
		val, err := r.readValue(t)
		if err != nil {
			return err
		}
		field.Set(*val)
	}

	return r.expectByte(bencodeEnd)
}
