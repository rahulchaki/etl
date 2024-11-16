package etl

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
)

func JSON_DECODER[T any](data []byte) (*T, error) {
	var record T
	err := json.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

type PartitionRecordBatch[T any] struct {
	Shard        string
	Partition    string
	Offset       interface{}
	Records      []*T
	continuation string
}

func (p *PartitionRecordBatch[T]) Id() string {
	if p.continuation != "" {
		return p.continuation
	}
	offsetText, err := json.Marshal(p.Offset)
	if err != nil {
		offsetText = []byte("error")
	}
	p.continuation = fmt.Sprintf("%s__%s__%s", p.Shard, p.Partition, string(offsetText))
	return p.continuation
}

func (p *PartitionRecordBatch[T]) RecordId(recordIdx int) string {
	if p.continuation == "" {
		p.continuation = p.Id()
	}
	return fmt.Sprintf("%s__%d", p.continuation, recordIdx)
}

type ElementPartition[T any] interface {
	Id() string
	Done() bool
	NextBatch(resource interface{}, batchSize int) ([]*T, interface{}, error)
	Close() error
}

type Closeable interface {
	Close() error
}

type ElementShard[T any] interface {
	Id() string
	NewResource() (Closeable, error)
	Partitions() ([]ElementPartition[T], error)
}

type ElementSource[T any] interface {
	Id() string
	Shards() ([]ElementShard[T], error)
}

type SliceSource[T any] struct {
	id         string
	data       []*T
	partitions int
}

func NewSliceSource[T any](data []*T, partitions int) ElementSource[T] {
	return &SliceSource[T]{
		id:         uuid.New().String(),
		data:       data,
		partitions: partitions,
	}
}

func (s *SliceSource[T]) Id() string {
	return s.id
}

func (s *SliceSource[T]) Shards() ([]ElementShard[T], error) {
	var shards []ElementShard[T]
	chunks := buildEqualChunks(s.data, s.partitions)
	for i, chunk := range chunks {
		shards = append(shards, &SliceShard[T]{
			id:   fmt.Sprintf("%s__%d", s.id, i),
			data: chunk,
		})
	}
	return shards, nil
}

type SliceShard[T any] struct {
	id   string
	data []*T
}

func (s *SliceShard[T]) Id() string {
	return s.id
}
func (s *SliceShard[T]) NewResource() (Closeable, error) {
	return nil, nil
}
func (s *SliceShard[T]) Partitions() ([]ElementPartition[T], error) {
	return []ElementPartition[T]{&SlicePartition[T]{
		id:   s.Id(),
		data: s.data,
	}}, nil
}

type SlicePartition[T any] struct {
	id     string
	data   []*T
	offset int
}

func (s *SlicePartition[T]) Id() string {
	return s.id
}
func (s *SlicePartition[T]) Done() bool {
	return s.offset >= len(s.data)
}
func (s *SlicePartition[T]) NextBatch(resource interface{}, batchSize int) ([]*T, interface{}, error) {
	start := s.offset
	end := min(s.offset+batchSize, len(s.data))
	s.offset = end
	return s.data[start:end], s.offset, nil
}
func (s *SlicePartition[T]) Close() error {
	return nil
}
