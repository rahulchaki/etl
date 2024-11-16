package etl

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type ProcessedRecord struct {
	Id     any
	Record interface{}
	Err    error
}

type ShardWorker[T any] struct {
	Id     string
	logger *zap.Logger
	buffer chan PartitionRecordBatch[T]
}

func NewShardWorker[T any](
	id string,
	readBufferSize int,
	logger *zap.Logger,
) *ShardWorker[T] {
	return &ShardWorker[T]{
		ctx:    ctx,
		Id:     id,
		logger: logger,
		buffer: make(chan PartitionRecordBatch[T], readBufferSize),
	}
}

func (s *ShardWorker[T]) Produce(
	processor ElementProcessor[T],
	sinkFactory ElementWriterFactory,
	writeParallelism int,
	notifyUpdateTo func(WorkerMetrics),
) error {
	var tasks errgroup.Group
	for i := range writeParallelism {
		tasks.Go(func() error {
			producerName := fmt.Sprintf("%s_producer_%d", s.Id, i)
			logger := s.logger.With(zap.String("producer", producerName))
			sink, err := sinkFactory(producerName)
			if err != nil {
				return err
			}

			defer func(sink ElementWriter) {
				err := sink.Close()
				if err != nil {
					logger.Error("Error closing sink", zap.Error(err))
				}
			}(sink)
			logger.Info("Shard Producer started")

		Loop:
			for {
				select {
				case inputBatch, ok := <-s.buffer:
					if !ok {
						logger.Info("Shard BufferChannel closed")
						break Loop
					}
					metrics := WorkerMetrics{}
					transformedBatch, err := processor.ProcessBatch(inputBatch.Records)
					if err != nil {
						logger.Error("Error processing batch", zap.Error(err))
						var batchErrors []*ProcessedRecord
						for i := range len(inputBatch.Records) {
							batchErrors = append(batchErrors, &ProcessedRecord{
								Id:  inputBatch.RecordId(i),
								Err: err,
							})
						}
						transformedBatch = batchErrors
					}
					for _, output := range transformedBatch {
						metrics.Processed++
						if output.Err != nil {
							metrics.Errors++
							_ = sink.AppendError(output.Id, output.Err)
						} else {
							metrics.Successes++
							_ = sink.Append(output.Id, output.Record)
						}
					}
					notifyUpdateTo(metrics)
				}
			}
			logger.Info("Shard Producer finished")
			return nil
		})
	}
	err := tasks.Wait()
	s.logger.Info("Shard Sink finished", zap.Error(err))
	return err
}

func (s *ShardWorker[T]) Consume(
	ctx context.Context,
	shard ElementShard[T],
	readParallelism int,
	readBatchSize int,
	notifyUpdateTo func(WorkerMetrics),
	maxBatchesPerChunk int,
) error {
	defer close(s.buffer)
	partitions, err := shard.Partitions()
	if err != nil {
		return err
	}
	parallelism := min(readParallelism, len(partitions))
	chunks := buildEqualChunks(partitions, parallelism)
	s.logger.Info("Shard Consumer chunks", zap.Int("partitions", len(partitions)), zap.Int("chunks", len(chunks)), zap.Int("parallelism", parallelism))
	var tasks errgroup.Group
	for i, partitionsInChunk := range chunks {
		logger := s.logger.With(zap.Int("chunk", i))
		tasks.Go(func() error {

			defer func(parts []ElementPartition[T]) {
				for _, partition := range parts {
					_ = partition.Close()
				}
			}(chunks[i])

			resource, err := shard.NewResource()
			if err != nil {
				return err
			}
			defer func(resource Closeable) {
				if resource != nil {
					_ = resource.Close()
				}
			}(resource)

			logger.Info("Starting Shard Consumer chunk", zap.Int("partitions", len(partitionsInChunk)))

			batchesToBeFetched := 0
			pendingWork := true
			for pendingWork && (maxBatchesPerChunk == 0 || batchesToBeFetched <= maxBatchesPerChunk) {
				select {
				case <-s.ctx.Done():
					return nil
				default:
				}
				pendingWork = false
				for _, partition := range partitionsInChunk {
					if !partition.Done() {
						pendingWork = true
						recordsBatch, offset, err := partition.NextBatch(resource, readBatchSize)
						batchesToBeFetched++
						if err != nil {
							return err
						}
						if recordsBatch == nil {
							continue
						}
						s.buffer <- PartitionRecordBatch[T]{
							Shard:     s.Id,
							Partition: partition.Id(),
							Offset:    offset,
							Records:   recordsBatch,
						}
						notifyUpdateTo(WorkerMetrics{
							Processed: len(recordsBatch),
							Successes: len(recordsBatch),
							Errors:    0,
						})
					} else {
						logger.Info("Partition done in chunk", zap.String("partition", partition.Id()))
					}
				}
			}
			logger.Info("Shard Consumer chunk finished")
			return nil
		})
	}
	err = tasks.Wait()
	return err
}

func buildEqualChunks[T any](items []T, numChunks int) [][]T {
	chunkSize := max(len(items)/numChunks, 1)
	var chunks [][]T
	i := 0
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
		i++
	}
	chunks = append(chunks, items)
	return chunks
}
