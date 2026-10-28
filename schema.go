package actorxdist

import (
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/go-aie/actorx"
)

// wireContract contains canonical bytes, never business receivers or closures.
type wireContract struct {
	data        []byte
	fingerprint string
}

type schemaBuilder struct {
	ids      map[reflect.Type]string
	nodes    map[string]any
	visiting map[reflect.Type]bool
	steps    int
	size     int
}

func buildContract(descriptions []actorx.CallDescriptor) (wireContract, error) {
	b := schemaBuilder{ids: map[reflect.Type]string{}, nodes: map[string]any{}, visiting: map[reflect.Type]bool{}}
	handlers := make([][]any, 0, len(descriptions))
	if len(descriptions) > 64 {
		return wireContract{}, actorx.ErrSetup
	}
	for _, d := range descriptions {
		message, err := b.typ(d.MessageType(), 0)
		if err != nil {
			return wireContract{}, err
		}
		var result any
		if d.ResultType() != nil {
			result, err = b.typ(d.ResultType(), 0)
			if err != nil {
				return wireContract{}, err
			}
		}
		modes := []string{"send", "call"}
		if d.Validate(actorx.StreamKind) == nil {
			modes = []string{"stream"}
		}
		handlers = append(handlers, []any{message, result, modes})
	}

	// Sort both collections so binding order and map iteration cannot change the fingerprint.
	slices.SortFunc(handlers, func(a, b []any) int { return strings.Compare(a[0].(string), b[0].(string)) })
	keys := make([]string, 0, len(b.nodes))
	for k := range b.nodes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	definitions := make([][]any, 0, len(keys))
	for _, k := range keys {
		definitions = append(definitions, []any{k, b.nodes[k]})
	}

	data, err := json.Marshal([]any{1, handlers, definitions})
	if err != nil || len(data) > 64<<10 {
		return wireContract{}, actorx.ErrSetup
	}
	sum := sha256.Sum256(data)
	return wireContract{data, hex.EncodeToString(sum[:])}, nil
}

func (b *schemaBuilder) typ(t reflect.Type, depth int) (string, error) {
	bad := func() (string, error) { return "", fmt.Errorf("%w: unsupported wire type %v", actorx.ErrSetup, t) }
	b.steps++
	if t == nil || depth > 32 || b.steps > 8192 || t.Size() > 128<<10 {
		return bad()
	}
	if id, ok := b.ids[t]; ok {
		return id, nil
	}
	if t == reflect.TypeFor[Addr]() {
		b.ids[t] = "addr-v1"
		b.nodes["addr-v1"] = []any{"addr-v1"}
		return "addr-v1", nil
	}

	// Reject execution capabilities and custom codecs before inspecting fields.
	base := t
	pointers := map[reflect.Type]bool{}
	for base.Kind() == reflect.Pointer {
		if pointers[base] {
			return bad()
		}
		pointers[base] = true
		base = base.Elem()
	}
	if base != reflect.TypeFor[Addr]() &&
		(base.PkgPath() == "github.com/go-aie/actorx" ||
			(base.PkgPath() == "github.com/go-aie/actorx-dist" &&
				(base.Name() == "Runtime" || base.Name() == "Context" || base.Name() == "Addr"))) {
		return bad()
	}
	for _, cap := range []reflect.Type{
		reflect.TypeFor[json.Marshaler](),
		reflect.TypeFor[json.Unmarshaler](),
		reflect.TypeFor[encoding.TextMarshaler](),
		reflect.TypeFor[encoding.TextUnmarshaler](),
	} {
		if base != reflect.TypeFor[Addr]() && (t.Implements(cap) || (t.Kind() != reflect.Pointer && reflect.PointerTo(t).Implements(cap))) {
			return bad()
		}
	}
	named := t.Name() != "" && t.PkgPath() != ""
	id := ""
	if named {
		if strings.ContainsAny(t.Name(), "[]") {
			return bad()
		}
		p, _ := json.Marshal(t.PkgPath())
		n, _ := json.Marshal(t.Name())
		id = "named(" + string(p) + "," + string(n) + ")"
		b.ids[t] = id // named recursive nodes terminate through this reference
	} else if b.visiting[t] {
		if t.Kind() == reflect.Pointer && b.ids[t.Elem()] != "" {
			return "ptr(" + b.ids[t.Elem()] + ")", nil
		}
		return bad()
	}
	b.visiting[t] = true
	defer delete(b.visiting, t)

	var node any
	switch t.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		node = []any{"scalar", t.Kind().String()}
		if id == "" {
			id = t.Kind().String()
		}
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		if t.Kind() == reflect.Map && t.Key() != reflect.TypeFor[string]() {
			return bad()
		}
		elem, err := b.typ(t.Elem(), depth+1)
		if err != nil {
			return "", err
		}
		switch t.Kind() {
		case reflect.Pointer:
			node = []any{"ptr", elem}
			if id == "" {
				id = "ptr(" + elem + ")"
			}
		case reflect.Slice:
			node = []any{"slice", elem}
			if id == "" {
				id = "slice(" + elem + ")"
			}
			if t.Elem() == reflect.TypeFor[byte]() {
				node = []any{"bytes"}
			}
		case reflect.Array:
			node = []any{"array", t.Len(), elem}
			if id == "" {
				id = fmt.Sprintf("array(%d,%s)", t.Len(), elem)
			}
		case reflect.Map:
			node = []any{"map", elem}
			if id == "" {
				id = "map(string," + elem + ")"
			}
		}
	case reflect.Struct:
		fields := make([][]any, 0, min(t.NumField(), 8192))
		fieldsBytes := 2
		seen := map[string]bool{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Anonymous {
				return bad()
			}
			if !f.IsExported() {
				continue
			}
			tag := strings.Split(f.Tag.Get("json"), ",")
			if tag[0] == "-" {
				if len(tag) != 1 {
					return bad()
				}
				continue
			}
			name := tag[0]
			if name == "" {
				name = f.Name
			}
			if len(name) > 1024 || !utf8.ValidString(name) {
				return bad()
			}
			optional := false
			if len(tag) > 2 || (len(tag) == 2 && tag[1] != "omitempty") {
				return bad()
			}
			if len(tag) == 2 {
				optional = true
			}
			if seen[name] {
				return bad()
			}
			seen[name] = true
			fieldID, err := b.typ(f.Type, depth+1)
			if err != nil {
				return "", err
			}
			field := []any{name, fieldID, optional}
			raw, _ := json.Marshal(field)
			fieldsBytes += len(raw) + 1
			if fieldsBytes > 64<<10 {
				return bad()
			}
			fields = append(fields, field)
		}
		slices.SortFunc(fields, func(a, b []any) int { return strings.Compare(a[0].(string), b[0].(string)) })
		node = []any{"struct", fields}
		if id == "" {
			raw, _ := json.Marshal(fields)
			id = "struct(" + string(raw) + ")"
		}
	default:
		return bad()
	}

	if len(id) > 1024 || (len(b.nodes) >= 256 && b.nodes[id] == nil) {
		return bad()
	}
	raw, _ := json.Marshal(node)
	quoted, _ := json.Marshal(id)
	b.size += len(raw) + len(quoted) + 4
	if b.size > 64<<10 {
		return bad()
	}
	b.ids[t] = id
	b.nodes[id] = node
	return id, nil
}
