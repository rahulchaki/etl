package etl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type ElementProcessor[T any] interface {
	Process(*T) *ProcessedRecord
	ProcessBatch([]*T) ([]*ProcessedRecord, error)
}

type IdentityMapper[T any] struct {
}

func NewIdentityMapper[T any]() ElementProcessor[DBRecord[T]] {
	return &IdentityMapper[T]{}
}

func (s IdentityMapper[T]) ProcessBatch(records []*DBRecord[T]) ([]*ProcessedRecord, error) {
	var processedRecords []*ProcessedRecord
	for _, record := range records {
		processedRecords = append(processedRecords, s.Process(record))
	}
	return processedRecords, nil
}

func (s IdentityMapper[T]) Process(record *DBRecord[T]) *ProcessedRecord {
	return &ProcessedRecord{
		Id:     record.Id,
		Record: record,
	}
}

type HTTPMapper[T any] struct {
	host             string
	path             string
	toRequestPayload func(record []*T) ([]byte, error)
}

func NewHTTPPostMapper[T any](host string, path string, toRequestPayload func(record []*T) ([]byte, error)) ElementProcessor[T] {
	return &HTTPMapper[T]{
		host:             host,
		path:             path,
		toRequestPayload: toRequestPayload,
	}

}
func (s *HTTPMapper[T]) Process(t *T) *ProcessedRecord {
	results, err := s.ProcessBatch([]*T{t})
	if err != nil || len(results) == 0 {
		return nil
	}
	return results[0]
}
func (s *HTTPMapper[T]) ProcessBatch(records []*T) ([]*ProcessedRecord, error) {
	if len(records) == 0 {
		return []*ProcessedRecord{}, nil
	}
	requestPayload, err := s.toRequestPayload(records)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Post(
		fmt.Sprintf("http://%s/%s", s.host, s.path),
		"application/json",
		bytes.NewBuffer(requestPayload),
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", response.StatusCode)
	}
	var transformedRecords []map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&transformedRecords)
	if err != nil {
		return nil, err
	}
	var processedRecords []*ProcessedRecord
	for _, record := range transformedRecords {
		processedRecords = append(processedRecords, &ProcessedRecord{
			Id:     record["Id"],
			Record: record,
		})
	}
	return processedRecords, nil
}
