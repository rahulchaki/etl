package etl

import (
	"context"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func ExecuteAll[T any](
	ctx context.Context,
	source ElementSource[T],
	readParallelismPerShard int,
	writeParallelismPerShard int,
	readBufferSize int,
	readRecordsBatchSize int,
	processor ElementProcessor[T],
	sinkFactory ElementWriterFactory,
	logger *zap.Logger,
) error {
	shards, err := source.Shards()
	if err != nil {
		return err
	}
	logger.Info("Processing shards", zap.Int("count", len(shards)))
	readProgressUpdater, writeProgressUpdater := buildProgressUpdaters(len(shards)*readParallelismPerShard, len(shards)*writeParallelismPerShard)

	var tasks errgroup.Group
	for _, shard := range shards {
		l := logger.With(zap.String("shard", shard.Id()))
		worker := NewShardWorker[T](shard.Id(), readBufferSize, l)
		tasks.Go(func() error {
			err := worker.Consume(ctx, shard, readParallelismPerShard, readRecordsBatchSize, func(metrics WorkerMetrics) {
				readProgressUpdater.Updates <- map[string]WorkerMetrics{shard.Id(): metrics}
			}, 0)
			if err != nil {
				l.Error("Error consuming shard", zap.Error(err))
				return err
			}

			return nil
		})
		tasks.Go(func() error {
			err := worker.Produce(ctx, processor, sinkFactory, writeParallelismPerShard, func(metrics WorkerMetrics) {
				writeProgressUpdater.Updates <- map[string]WorkerMetrics{shard.Id(): metrics}
			})
			if err != nil {
				l.Error("Error producing shard", zap.Error(err))
				return err
			}
			return nil
		})
	}
	var progressUpdatesTasks errgroup.Group
	readProgressUpdater.Run(&progressUpdatesTasks, 5, func(diff []ShardMetrics) {
		logger.Info("Read progress", zap.Any("progress", diff))
	})
	writeProgressUpdater.Run(&progressUpdatesTasks, 5, func(diff []ShardMetrics) {
		logger.Info("Write progress changes", zap.Any("progress", diff))
	})

	err = tasks.Wait()
	logger.Info("All shards processed", zap.Error(err))
	readProgressUpdater.Close()
	writeProgressUpdater.Close()
	_ = progressUpdatesTasks.Wait()
	logger.Info(" Reads Stats", zap.Any("stats", readProgressUpdater.Stats()))
	logger.Info(" Write Stats", zap.Any("stats", writeProgressUpdater.Stats()))
	return err
}

func buildProgressUpdaters(readBuffer, writeBuffer int) (*ProgressUpdater, *ProgressUpdater) {
	readUpdater := NewProgressUpdater(2*time.Second, readBuffer)
	writeUpdater := NewProgressUpdater(2*time.Second, writeBuffer)
	return readUpdater, writeUpdater
}
