package etl

import (
	"bufio"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
)

type filesShard[T any] struct {
	shard      string
	partitions []ElementPartition[T]
}

func NewFilesShard[T any](shard string, partitions []ElementPartition[T]) (ElementShard[T], error) {
	return &filesShard[T]{
		shard:      shard,
		partitions: partitions,
	}, nil
}

func (s *filesShard[T]) Id() string {
	return s.shard
}

func (s *filesShard[T]) NewResource() (Closeable, error) {
	return nil, nil
}

func (s *filesShard[T]) Partitions() ([]ElementPartition[T], error) {
	return s.partitions, nil
}

type directorySource[T any] struct {
	id        string
	directory string

	shards []ElementShard[T]
}

func (s *directorySource[T]) Id() string {
	return s.id
}

func (s *directorySource[T]) Shards() ([]ElementShard[T], error) {
	return s.shards, nil
}

func NewDirectorySourceSingleShard[T any](directory string, pattern string, decoder func(data []byte) (*T, error)) (ElementSource[T], error) {
	files, err := filepath.Glob(directory + "/" + pattern)
	if err != nil {
		return nil, err
	}
	var partitions []ElementPartition[T]
	for _, file := range files {
		partition, err := NewFileElementReaderAutoCompressed[T](file, decoder)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}
	shard, err := NewFilesShard[T](directory, partitions)
	return &directorySource[T]{
		id:        directory,
		directory: directory,
		shards:    []ElementShard[T]{shard},
	}, nil
}

func NewDirectorySource[T any](directory string, decoder func(data []byte) (*T, error)) (ElementSource[T], error) {
	filesPerShard := make(map[string][]ElementPartition[T])
	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if strings.HasSuffix(info.Name(), ".gz") {
				shard := strings.Split(info.Name(), "_")[0]
				if _, ok := filesPerShard[shard]; !ok {
					filesPerShard[shard] = make([]ElementPartition[T], 0)
				}
				reader, err := NewFileElementReaderAutoCompressed[T](path, decoder)
				if err != nil {
					return err
				}
				filesPerShard[shard] = append(filesPerShard[shard], reader)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var shards []ElementShard[T]
	for shard, partitions := range filesPerShard {
		partition, err := NewFilesShard[T](shard, partitions)
		if err != nil {
			return nil, err
		}
		shards = append(shards, partition)
	}
	return &directorySource[T]{
		id:        directory,
		directory: directory,
		shards:    shards,
	}, nil
}

type FileElementReader[T any] struct {
	path       string
	compressed bool
	decoder    func(data []byte) (*T, error)
	offset     int
	isDone     bool
	file       *os.File
	gzip       *gzip.Reader
	scanner    *bufio.Scanner
}

func NewFileElementReaderAutoCompressed[T any](path string, decoder func(data []byte) (*T, error)) (ElementPartition[T], error) {
	return NewFileElementReader[T](path, strings.HasSuffix(path, ".gz"), decoder)
}

func NewFileElementReader[T any](path string, compressed bool, decoder func(data []byte) (*T, error)) (ElementPartition[T], error) {

	var (
		file      *os.File
		zipReader *gzip.Reader
		scanner   *bufio.Scanner
		err       error
	)

	file, err = os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	if !compressed {
		scanner = bufio.NewScanner(file)
	} else {
		zipReader, err = gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		scanner = bufio.NewScanner(zipReader)
	}
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 128*1024*1024)
	scanner.Split(bufio.ScanLines)

	return &FileElementReader[T]{
		path:       path,
		compressed: compressed,
		decoder:    decoder,
		offset:     0,
		isDone:     false,
		file:       file,
		gzip:       zipReader,
		scanner:    scanner,
	}, nil
}

func (r *FileElementReader[T]) Id() string {
	return r.path
}

func (r *FileElementReader[T]) Done() bool {
	return r.isDone
}

func (r *FileElementReader[T]) NextBatch(resource interface{}, batchSize int) ([]*T, interface{}, error) {
	if r.isDone {
		return nil, r.offset, nil
	}
	batch := make([]*T, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		if r.scanner.Scan() {
			//err := json.Unmarshal(r.scanner.Bytes(), &data)
			bytes := r.scanner.Bytes()
			r.offset = r.offset + 1
			data, err := r.decoder(bytes)
			if err != nil {
				return nil, nil, err
			}
			batch = append(batch, data)
		} else {
			break
		}
	}
	if len(batch) == 0 {
		r.isDone = true
		return nil, r.offset, r.Close()
	}
	return batch, r.offset, nil
}

func (r *FileElementReader[T]) Close() error {
	if r.gzip != nil {
		_ = r.gzip.Close()
	}
	return r.file.Close()
}
