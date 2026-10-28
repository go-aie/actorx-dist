package actorxdist

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/strictjson"
)

const maxPayload = 64 << 10

// valueCodec never calls application codecs. Schema validation precedes use;
// traversal and the output writer are bounded before allocation/append.
type valueCodec struct {
	rt        *Runtime
	nodes     int
	data      []byte
	allocated uint64
}

func (c *valueCodec) step(depth int) error {
	c.nodes++
	if depth > 32 || c.nodes > 8192 {
		return actorx.ErrChunkSize
	}
	return nil
}

func (c *valueCodec) reserve(n uint64) error {
	if n > 128<<10-c.allocated {
		return actorx.ErrChunkSize
	}
	c.allocated += n
	return nil
}

func (c *valueCodec) put(s string) error {
	if len(s) > maxPayload-len(c.data) {
		return actorx.ErrChunkSize
	}
	c.data = append(c.data, s...)
	return nil
}

func (c *valueCodec) quote(s string) error {
	if !utf8.ValidString(s) {
		return actorx.ErrSetup
	}
	if len(s) > maxPayload-len(c.data)-2 {
		return actorx.ErrChunkSize
	}
	if err := c.put(`"`); err != nil {
		return err
	}
	for _, r := range s {
		if r >= 0x20 && r < 0x7f && r != '"' && r != '\\' && r != '<' && r != '>' && r != '&' {
			if len(c.data) == maxPayload {
				return actorx.ErrChunkSize
			}
			c.data = append(c.data, byte(r))
			continue
		}

		// At most twelve bytes of temporary JSON, including surrogate escapes.
		b, _ := json.Marshal(string(r))
		if err := c.put(string(b[1 : len(b)-1])); err != nil {
			return err
		}
	}
	return c.put(`"`)
}

func fieldName(f reflect.StructField) (string, bool) {
	tag := strings.Split(f.Tag.Get("json"), ",")
	name := tag[0]
	if name == "" {
		name = f.Name
	}
	return name, len(tag) == 2
}

func emptyJSON(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

func (c *valueCodec) encode(v reflect.Value, depth int) error {
	if err := c.step(depth); err != nil {
		return err
	}
	if !v.IsValid() {
		return c.put("null")
	}
	if v.Type() == reflect.TypeFor[Addr]() {
		b, err := v.Interface().(Addr).MarshalJSON()
		if err != nil {
			return err
		}
		return c.put(string(b))
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return c.put("null")
		}
		return c.encode(v.Elem(), depth+1)
	case reflect.String:
		return c.quote(v.String())
	case reflect.Bool:
		return c.put(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		x := v.Int()
		if v.Kind() == reflect.Int && (x < math.MinInt32 || x > math.MaxInt32) {
			return actorx.ErrSetup
		}
		return c.put(strconv.FormatInt(x, 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x := v.Uint()
		if v.Kind() == reflect.Uint && x > math.MaxUint32 {
			return actorx.ErrSetup
		}
		return c.put(strconv.FormatUint(x, 10))
	case reflect.Float32, reflect.Float64:
		x := v.Float()
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return actorx.ErrSetup
		}
		return c.put(strconv.FormatFloat(x, 'g', -1, v.Type().Bits()))
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return c.put("null")
		}
		if v.Kind() == reflect.Slice && v.Type().Elem() == reflect.TypeFor[byte]() {
			if base64.StdEncoding.EncodedLen(v.Len()) > maxPayload-len(c.data)-2 {
				return actorx.ErrChunkSize
			}
			return c.quote(base64.StdEncoding.EncodeToString(v.Bytes()))
		}
		if v.Len() > 8192-c.nodes {
			return actorx.ErrChunkSize
		}
		if err := c.put("["); err != nil {
			return err
		}
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				if err := c.put(","); err != nil {
					return err
				}
			}
			if err := c.encode(v.Index(i), depth+1); err != nil {
				return err
			}
		}
		return c.put("]")
	case reflect.Map:
		if v.IsNil() {
			return c.put("null")
		}
		if v.Len() > 8192-c.nodes {
			return actorx.ErrChunkSize
		}
		if err := c.put("{"); err != nil {
			return err
		}
		it := v.MapRange()
		i := 0
		for it.Next() {
			if i > 0 {
				if err := c.put(","); err != nil {
					return err
				}
			}
			i++
			if err := c.quote(it.Key().String()); err != nil {
				return err
			}
			if err := c.put(":"); err != nil {
				return err
			}
			if err := c.encode(it.Value(), depth+1); err != nil {
				return err
			}
		}
		return c.put("}")
	case reflect.Struct:
		if err := c.put("{"); err != nil {
			return err
		}
		count := 0
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			name, optional := fieldName(f)
			if !f.IsExported() || name == "-" || optional && emptyJSON(v.Field(i)) {
				continue
			}
			if count > 0 {
				if err := c.put(","); err != nil {
					return err
				}
			}
			count++
			if err := c.quote(name); err != nil {
				return err
			}
			if err := c.put(":"); err != nil {
				return err
			}
			if err := c.encode(v.Field(i), depth+1); err != nil {
				return err
			}
		}
		return c.put("}")
	}
	return actorx.ErrSetup
}

func encodeValue(value any) ([]byte, error) {
	c := valueCodec{data: make([]byte, 0, maxPayload)}
	err := c.encode(reflect.ValueOf(value), 0)
	if err != nil {
		return nil, err
	}
	return c.data, nil
}

func decodeValue(rt *Runtime, data []byte, t reflect.Type) (any, error) {
	if len(data) > maxPayload {
		return nil, actorx.ErrChunkSize
	}
	if strictjson.Validate(data, maxPayload, 40, 32768) != nil {
		return nil, ErrProtocol
	}
	c := valueCodec{rt: rt}
	v, err := c.decode(bytes.TrimSpace(data), t, 0)
	if err != nil {
		return nil, err
	}
	return v.Interface(), nil
}

func (c *valueCodec) decode(data []byte, t reflect.Type, depth int) (reflect.Value, error) {
	// Charge decoded storage before allocation; a small payload can describe a large value.
	if uint64(t.Size()) > 128<<10-c.allocated {
		return reflect.Value{}, actorx.ErrChunkSize
	}
	c.allocated += uint64(t.Size())
	out := reflect.New(t).Elem()
	bad := func() (reflect.Value, error) { return out, ErrProtocol }
	if err := c.step(depth); err != nil {
		return out, err
	}
	if t == reflect.TypeFor[Addr]() {
		a, err := c.rt.DecodeAddr(data)
		return reflect.ValueOf(a), err
	}
	if bytes.Equal(data, []byte("null")) {
		if t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
			return out, nil
		}
		return bad()
	}

	switch t.Kind() {
	case reflect.Pointer:
		v, err := c.decode(data, t.Elem(), depth+1)
		if err != nil {
			return out, err
		}
		out.Set(reflect.New(t.Elem()))
		out.Elem().Set(v)
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &fields) != nil {
			return bad()
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name, optional := fieldName(f)
			if !f.IsExported() || name == "-" {
				continue
			}
			raw, ok := fields[name]
			if !ok {
				if optional {
					continue
				}
				return bad()
			}
			v, err := c.decode(raw, f.Type, depth+1)
			if err != nil {
				return out, err
			}
			out.Field(i).Set(v)
			delete(fields, name)
		}
		if len(fields) != 0 {
			return bad()
		}
	case reflect.Map:
		var fields map[string]json.RawMessage
		if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &fields) != nil {
			return bad()
		}
		if len(fields) > 8192-c.nodes {
			return bad()
		}
		cost := uint64(len(fields)) * (uint64(t.Elem().Size()) + 64)
		for key := range fields {
			cost += uint64(len(key))
		}
		if cost > 128<<10-c.allocated {
			return out, actorx.ErrChunkSize
		}
		c.allocated += cost
		out.Set(reflect.MakeMapWithSize(t, len(fields)))
		for k, raw := range fields {
			v, err := c.decode(raw, t.Elem(), depth+1)
			if err != nil {
				return out, err
			}
			out.SetMapIndex(reflect.ValueOf(k), v)
		}
	case reflect.Slice, reflect.Array:
		if t.Kind() == reflect.Slice && t.Elem() == reflect.TypeFor[byte]() {
			if err := c.reserve(uint64(len(data) + base64.StdEncoding.DecodedLen(len(data)))); err != nil {
				return out, err
			}
			var s string
			if json.Unmarshal(data, &s) != nil || strings.ContainsAny(s, "\r\n") {
				return bad()
			}
			b, err := base64.StdEncoding.Strict().DecodeString(s)
			if err != nil || base64.StdEncoding.EncodeToString(b) != s {
				return bad()
			}
			out.SetBytes(b)
			return out, nil
		}
		var items []json.RawMessage
		if len(data) == 0 || data[0] != '[' || json.Unmarshal(data, &items) != nil || len(items) > 8192-c.nodes {
			return bad()
		}
		if t.Kind() == reflect.Array {
			if len(items) != t.Len() {
				return bad()
			}
		} else {
			cost := uint64(len(items)) * uint64(t.Elem().Size())
			if cost > 128<<10-c.allocated {
				return out, actorx.ErrChunkSize
			}
			c.allocated += cost
			out.Set(reflect.MakeSlice(t, len(items), len(items)))
		}
		for i, raw := range items {
			v, err := c.decode(raw, t.Elem(), depth+1)
			if err != nil {
				return out, err
			}
			out.Index(i).Set(v)
		}
	case reflect.String:
		if err := c.reserve(uint64(len(data))); err != nil {
			return out, err
		}
		var s string
		if json.Unmarshal(data, &s) != nil {
			return bad()
		}
		out.SetString(s)
	case reflect.Bool:
		var b bool
		if json.Unmarshal(data, &b) != nil {
			return bad()
		}
		out.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		bits := t.Bits()
		if t.Kind() == reflect.Int {
			bits = 32
		}
		x, err := strconv.ParseInt(string(data), 10, bits)
		if err != nil {
			return bad()
		}
		out.SetInt(x)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		bits := t.Bits()
		if t.Kind() == reflect.Uint {
			bits = 32
		}
		x, err := strconv.ParseUint(string(data), 10, bits)
		if err != nil {
			return bad()
		}
		out.SetUint(x)
	case reflect.Float32, reflect.Float64:
		x, err := strconv.ParseFloat(string(data), t.Bits())
		if err != nil || math.IsInf(x, 0) || math.IsNaN(x) {
			return bad()
		}
		out.SetFloat(x)
	default:
		return bad()
	}
	return out, nil
}
