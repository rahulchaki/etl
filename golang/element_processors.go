package etl

import (
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
	requestBuilder func(record []*T) (*http.Request, error)
}

func NewHTTPPostMapper[T any](requestBuilder func(record []*T) (*http.Request, error)) ElementProcessor[T] {
	return &HTTPMapper[T]{
		requestBuilder: requestBuilder,
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
	request, err := s.requestBuilder(records)
	if err != nil {
		return nil, err
	}

	response, err := http.DefaultClient.Do(request)
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
