package etl

import (
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

type Numerical interface {
	Zero() Numerical
	Add(Numerical) Numerical
	Sub(Numerical) Numerical
}

type WorkerMetrics struct {
	Processed int
	Successes int
	Errors    int
}
type ShardMetrics struct {
	Shard   string
	Metrics WorkerMetrics
}

func NewShardMetricsFromMap(asMap map[string]WorkerMetrics) []ShardMetrics {
	metrics := make([]ShardMetrics, 0, len(asMap))
	for key, value := range asMap {
		metrics = append(metrics, ShardMetrics{key, value})
	}
	slices.SortFunc(metrics, func(i, j ShardMetrics) int { return j.Metrics.Successes - i.Metrics.Successes })
	return metrics
}

type ProgressUpdater struct {
	frequency time.Duration
	stats     map[string]WorkerMetrics
	Updates   chan map[string]WorkerMetrics
}

func NewProgressUpdater(
	frequency time.Duration,
	buffer int,
) *ProgressUpdater {
	return &ProgressUpdater{
		frequency: frequency,
		stats:     make(map[string]WorkerMetrics),
		Updates:   make(chan map[string]WorkerMetrics, buffer),
	}
}

func (p *ProgressUpdater) Close() {
	close(p.Updates)
}

func (p *ProgressUpdater) Run(taskGroup *errgroup.Group, topN int, notify func([]ShardMetrics)) {

	isZero := func(m WorkerMetrics) bool {
		return m.Processed == 0 && m.Successes == 0 && m.Errors == 0
	}

	addMetric := func(lhs WorkerMetrics, rhs WorkerMetrics) WorkerMetrics {
		return WorkerMetrics{
			Processed: lhs.Processed + rhs.Processed,
			Successes: lhs.Successes + rhs.Successes,
			Errors:    lhs.Errors + rhs.Errors,
		}
	}

	diffMetric := func(lhs WorkerMetrics, rhs WorkerMetrics) WorkerMetrics {
		return WorkerMetrics{
			Processed: lhs.Processed - rhs.Processed,
			Successes: lhs.Successes - rhs.Successes,
			Errors:    lhs.Errors - rhs.Errors,
		}
	}

	merge := func(progress map[string]WorkerMetrics, update map[string]WorkerMetrics) map[string]WorkerMetrics {
		for k, v := range update {
			existing, ok := progress[k]
			if !ok {
				progress[k] = v
				continue
			}
			progress[k] = addMetric(existing, v)
		}
		return progress
	}

	diff := func(latest map[string]WorkerMetrics, last map[string]WorkerMetrics) map[string]WorkerMetrics {
		result := make(map[string]WorkerMetrics)
		for k, v := range latest {
			lastV, ok := last[k]
			if !ok {
				result[k] = v
				continue
			}
			diff := diffMetric(v, lastV)
			if !isZero(diff) {
				result[k] = diff
			}
		}
		return result
	}

	taskGroup.Go(func() error {
		progress := make(map[string]WorkerMetrics)
		var lastProgress map[string]WorkerMetrics
		ticker := time.NewTicker(p.frequency)
		for {
			select {
			case <-ticker.C:
				changes := diff(progress, lastProgress)
				if len(changes) > 0 {
					if topN < 1 {
						notify(NewShardMetricsFromMap(changes))
					} else {
						notify(NewShardMetricsFromMap(changes)[:min(topN, len(changes))])
					}
				}
				lastProgress = make(map[string]WorkerMetrics)
				for k, v := range progress {
					lastProgress[k] = v
				}
			case u, ok := <-p.Updates:
				if !ok {
					ticker.Stop()
					p.stats = progress
					return nil
				}
				progress = merge(progress, u)
			}
		}
	})
}

func (p *ProgressUpdater) Stats() map[string]WorkerMetrics {
	return p.stats
}
