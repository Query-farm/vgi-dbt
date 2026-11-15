// Typed value helpers shared by every table: decoding raw JSON fields into the
// Go values the Arrow builders accept, and building projected record batches.

package dbt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Query-farm/vgi-go/vgi"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Arrow types used by the output schemas.
var (
	tString  = arrow.BinaryTypes.String
	tInt64   = arrow.PrimitiveTypes.Int64
	tInt32   = arrow.PrimitiveTypes.Int32
	tFloat64 = arrow.PrimitiveTypes.Float64
	tBool    = arrow.FixedWidthTypes.Boolean
	tTS      = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	tStrList = arrow.ListOf(arrow.BinaryTypes.String)
	tJSON    = mustJSONType()
)

func mustJSONType() arrow.DataType {
	t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		panic(err)
	}
	return t
}

// jsonText is a value destined for a JSON column.
type jsonText string

// isNullRaw reports whether a raw value is absent or JSON null.
func isNullRaw(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

// rawString decodes a JSON string; a non-string scalar is returned as its text.
func rawString(raw json.RawMessage) any {
	if isNullRaw(raw) {
		return nil
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
		return nil
	}
	if raw[0] == '{' || raw[0] == '[' {
		return string(compactJSON(raw))
	}
	return string(bytes.TrimSpace(raw))
}

func rawFloat(raw json.RawMessage) any {
	if isNullRaw(raw) {
		return nil
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		return f
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return nil
}

func rawInt(raw json.RawMessage) any {
	f := rawFloat(raw)
	if f == nil {
		return nil
	}
	v := f.(float64)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return int64(v)
}

func rawBool(raw json.RawMessage) any {
	if isNullRaw(raw) {
		return nil
	}
	var b bool
	if json.Unmarshal(raw, &b) == nil {
		return b
	}
	return nil
}

func rawStrings(raw json.RawMessage) any {
	if isNullRaw(raw) {
		return nil
	}
	var out []string
	if json.Unmarshal(raw, &out) == nil {
		return out
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return []string{one}
	}
	return nil
}

func compactJSON(raw json.RawMessage) []byte {
	var buf bytes.Buffer
	if json.Compact(&buf, raw) != nil {
		return raw
	}
	return buf.Bytes()
}

func rawJSON(raw json.RawMessage) any {
	if isNullRaw(raw) {
		return nil
	}
	return jsonText(compactJSON(raw))
}

// timeLayouts are the timestamp spellings dbt and dbt Cloud emit.
var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999Z0700",
	"2006-01-02",
}

// parseTime parses a dbt timestamp string; zone-less values are UTC.
func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// rawTime decodes a timestamp string or a float of epoch seconds.
func rawTime(raw json.RawMessage) any {
	if isNullRaw(raw) {
		return nil
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return nil
		}
		if t, ok := parseTime(s); ok {
			return t
		}
		return nil
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil && f > 0 {
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)).UTC()
	}
	return nil
}

// rawObject decodes an object into its fields; nil for anything else.
func rawObject(raw json.RawMessage) map[string]json.RawMessage {
	if isNullRaw(raw) || raw[0] != '{' {
		return nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	return m
}

// path walks nested objects: path(m, "config", "materialized").
func path(m map[string]json.RawMessage, keys ...string) json.RawMessage {
	var cur json.RawMessage
	for i, k := range keys {
		if m == nil {
			return nil
		}
		cur = m[k]
		if i < len(keys)-1 {
			m = rawObject(cur)
		}
	}
	return cur
}

// firstNonNull returns the first value that is not nil.
func firstNonNull(vs ...any) any {
	for _, v := range vs {
		if v != nil {
			return v
		}
	}
	return nil
}

// buildBatch turns full-width rows (values in fullSchema order) into a record
// batch with the (possibly projected) out schema.
func buildBatch(out *arrow.Schema, full *arrow.Schema, rows [][]any) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.NewGoAllocator(), out)
	defer b.Release()
	idx := make([]int, out.NumFields())
	for i, f := range out.Fields() {
		ix := full.FieldIndices(f.Name)
		if len(ix) == 0 {
			return nil, fmt.Errorf("internal: projected column %q not in schema", f.Name)
		}
		idx[i] = ix[0]
	}
	for _, row := range rows {
		for i := range idx {
			if err := appendValue(b.Field(i), row[idx[i]]); err != nil {
				return nil, fmt.Errorf("column %s: %w", out.Field(i).Name, err)
			}
		}
	}
	return b.NewRecordBatch(), nil
}

func appendValue(fb array.Builder, v any) error {
	if v == nil {
		fb.AppendNull()
		return nil
	}
	switch b := fb.(type) {
	case *array.StringBuilder:
		switch x := v.(type) {
		case string:
			b.Append(x)
		case jsonText:
			b.Append(string(x))
		default:
			b.Append(fmt.Sprint(x))
		}
	case *array.ExtensionBuilder:
		sb, ok := b.StorageBuilder().(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("unsupported extension storage")
		}
		switch x := v.(type) {
		case jsonText:
			sb.Append(string(x))
		default:
			enc, err := json.Marshal(x)
			if err != nil {
				return err
			}
			sb.Append(string(enc))
		}
	case *array.Int64Builder:
		switch x := v.(type) {
		case int64:
			b.Append(x)
		case int:
			b.Append(int64(x))
		case float64:
			b.Append(int64(x))
		default:
			return fmt.Errorf("want int64, got %T", v)
		}
	case *array.Int32Builder:
		switch x := v.(type) {
		case int64:
			b.Append(int32(x))
		case int:
			b.Append(int32(x))
		case int32:
			b.Append(x)
		default:
			return fmt.Errorf("want int32, got %T", v)
		}
	case *array.Float64Builder:
		switch x := v.(type) {
		case float64:
			b.Append(x)
		case int64:
			b.Append(float64(x))
		default:
			return fmt.Errorf("want float64, got %T", v)
		}
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		b.Append(x)
	case *array.TimestampBuilder:
		x, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("want time, got %T", v)
		}
		b.Append(arrow.Timestamp(x.UnixMicro()))
	case *array.ListBuilder:
		x, ok := v.([]string)
		if !ok {
			return fmt.Errorf("want []string, got %T", v)
		}
		b.Append(true)
		vb := b.ValueBuilder().(*array.StringBuilder)
		for _, s := range x {
			vb.Append(s)
		}
	default:
		return fmt.Errorf("unsupported builder %T", fb)
	}
	return nil
}

// rawList decodes an array into its elements; nil for anything else.
func rawList(raw json.RawMessage) []json.RawMessage {
	if isNullRaw(raw) || raw[0] != '[' {
		return nil
	}
	var out []json.RawMessage
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}

// durationSeconds is e - s in seconds when both are times.
func durationSeconds(s, e any) any {
	st, ok1 := s.(time.Time)
	et, ok2 := e.(time.Time)
	if !ok1 || !ok2 {
		return nil
	}
	return et.Sub(st).Seconds()
}

// argErr is a bind-time argument error with a plain message.
func argErr(msg string) error { return &vgi.ArgumentError{Position: -1, Detail: msg} }
