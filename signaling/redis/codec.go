package redis

import "encoding/json"

// Codec 定义信号编解码能力。
type Codec[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(data []byte, v *T) error
}

// JSONCodec 使用 JSON 编解码信号。
type JSONCodec[T any] struct{}

func (JSONCodec[T]) Marshal(v T) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec[T]) Unmarshal(data []byte, v *T) error {
	return json.Unmarshal(data, v)
}
