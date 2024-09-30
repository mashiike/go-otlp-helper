package otlp

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type JSONEncoder struct {
	writer    io.Writer
	marshaler protojson.MarshalOptions
}

func NewJSONEncoder(writer io.Writer) *JSONEncoder {
	return &JSONEncoder{
		writer: writer,
		marshaler: protojson.MarshalOptions{
			UseEnumNumbers:  true,
			EmitUnpopulated: false,
		},
	}
}

func (e *JSONEncoder) SetIndent(indent string) {
	e.marshaler.Multiline = true
	e.marshaler.Indent = indent
}
func (e *JSONEncoder) Encode(msg proto.Message) error {
	data, err := e.marshaler.Marshal(msg)
	if err != nil {
		return err
	}

	data = convertTraceIDAndSpanIDBase64ToHex(data)
	_, err = e.writer.Write(data)
	return err
}

func convertTraceIDAndSpanIDBase64ToHex(data []byte) []byte {
	var m any
	if err := json.Unmarshal(data, &m); err != nil {
		slog.Warn("failed to convert traceID and spanID from base64 to hex", "error", err.Error())
		return data
	}
	m = convertTraceIDAndSpanIDBase64ToHexForAny(m)
	converted, err := json.Marshal(m)
	if err != nil {
		slog.Warn("failed to convert traceID and spanID from base64 to hex", "error", err.Error())
		return data
	}
	return converted
}

func convertTraceIDAndSpanIDBase64ToHexForAny(data any) any {
	switch data := data.(type) {
	case map[string]interface{}:
		return convertTraceIDAndSpanIDBase64ToHexForMap(data)
	case []interface{}:
		for i, v := range data {
			data[i] = convertTraceIDAndSpanIDBase64ToHexForAny(v)
		}
	}
	return data
}

func keyIsTraceIDOrSpanID(k string) bool {
	return strings.EqualFold(strings.ReplaceAll(k, "_", ""), "traceId") || strings.EqualFold(strings.ReplaceAll(k, "_", ""), "spanId")
}

func convertTraceIDAndSpanIDBase64ToHexForMap(data map[string]interface{}) map[string]interface{} {
	for k, v := range data {
		if keyIsTraceIDOrSpanID(k) {
			if s, ok := v.(string); ok {
				bs, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					slog.Warn("failed to convert traceID and spanID from base64 to hex", "error", err.Error())
				} else {
					data[k] = strings.ToUpper(hex.EncodeToString(bs))
				}
				continue
			}
			slog.Warn("unexpected type of traceID and spanID", "key", k, "value_type", fmt.Sprintf("%T", v))
		}
		data[k] = convertTraceIDAndSpanIDBase64ToHexForAny(v)
	}
	return data
}

type JSONDecoder struct {
	dec  *json.Decoder
	opts protojson.UnmarshalOptions
}

func NewJSONDecoder(reader io.Reader) *JSONDecoder {
	return &JSONDecoder{
		dec:  json.NewDecoder(reader),
		opts: protojson.UnmarshalOptions{},
	}
}

func (d *JSONDecoder) More() bool {
	return d.dec.More()
}

func (d *JSONDecoder) Decode(msg proto.Message) error {
	var m any
	if err := d.dec.Decode(&m); err != nil {
		return err
	}
	m = convertTraceIDAndSpanIDHexToBase64ForAny(m)
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return d.opts.Unmarshal(data, msg)
}

func convertTraceIDAndSpanIDHexToBase64ForAny(data any) any {
	switch data := data.(type) {
	case map[string]interface{}:
		return convertTraceIDAndSpanIDHexToBase64ForMap(data)
	case []interface{}:
		for i, v := range data {
			data[i] = convertTraceIDAndSpanIDHexToBase64ForAny(v)
		}
	}
	return data
}

func convertTraceIDAndSpanIDHexToBase64ForMap(data map[string]interface{}) map[string]interface{} {
	for k, v := range data {
		if keyIsTraceIDOrSpanID(k) {
			if s, ok := v.(string); ok {
				bs, err := hex.DecodeString(s)
				if err != nil {
					slog.Warn("failed to convert traceID and spanID from hex to base64", "error", err.Error())
				} else {
					data[k] = base64.StdEncoding.EncodeToString(bs)
				}
				continue
			}
			slog.Warn("unexpected type of traceID and spanID", "key", k, "value_type", fmt.Sprintf("%T", v))
		}
		data[k] = convertTraceIDAndSpanIDHexToBase64ForAny(v)
	}
	return data
}
