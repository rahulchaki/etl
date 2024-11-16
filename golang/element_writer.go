package etl

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
)

type RecordEncoder = func(interface{}) ([]byte, error)

var ENCODER_JSON = json.Marshal

type ElementWriter interface {
	AppendError(any, error) error
	Append(any, interface{}) error
	Close() error
}

type fsSink struct {
	encoder RecordEncoder
	file    *os.File
	gzip    *gzip.Writer
	writer  *bufio.Writer
}

func NewFileElementWriter(path string, encoder RecordEncoder) (ElementWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	gz := gzip.NewWriter(file)
	writer := bufio.NewWriter(gz)
	return &fsSink{encoder, file, gz, writer}, nil
}

func (f *fsSink) append(data map[string]any) error {
	bytes, err := f.encoder(data)
	if err != nil {
		return err
	}
	_, err = f.writer.Write(bytes)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f.writer)
	return err
}

func (f *fsSink) Append(id any, data interface{}) error {
	idText, err := json.Marshal(id)
	if err != nil {
		return err
	}
	return f.append(map[string]any{"id": idText, "record": data})
}

func (f *fsSink) AppendError(id any, recordErr error) error {
	idText, err := json.Marshal(id)
	if err != nil {
		return err
	}
	return f.append(map[string]any{"id": idText, "error": recordErr})
}

func (f *fsSink) Close() error {
	flushErr := f.writer.Flush()
	flushErr = f.gzip.Flush()
	closeErr := f.gzip.Close()
	closeErr = f.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

type ElementWriterFactory = func(string) (ElementWriter, error)

func NewFSSinkFactory(directory string, encoder RecordEncoder) ElementWriterFactory {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
	return func(partitionKey string) (ElementWriter, error) {
		path := fmt.Sprintf("%s/%s.json.gz", directory, partitionKey)
		return NewFileElementWriter(path, encoder)
	}
}
