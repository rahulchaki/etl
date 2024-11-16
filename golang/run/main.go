package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"etl"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func RunETL(ctx context.Context, runId string, logger *zap.Logger) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	now := time.Now()
	outputDir := homeDir + "/data/deliveries_" + runId
	readParallelismPerShard := 10
	writeParallelismPerShard := 10
	recordBatchSize := 100
	sinkFactory := etl.NewFSSinkFactory(outputDir, etl.ENCODER_JSON)
	var hosts = CIO_HOSTS // []string{"localhost"}
	source, err := etl.NewMySQLSource[DeliveryDBRecord](hosts, "root", "production_env*", "delivs_2024_11")
	if err != nil {
		return err
	}

	logger.Info("Starting ETL", zap.String("outputDir", outputDir))

	err = etl.ExecuteAll(
		ctx,
		source,
		readParallelismPerShard,
		writeParallelismPerShard,
		100,
		recordBatchSize,
		cioDeliveryExtract(),
		sinkFactory,
		logger,
	)
	if err != nil {
		logger.Error("Error while Running All ", zap.Error(err), zap.Duration("duration", time.Since(now)))
		return err
	}
	logger.Info("Finished ETL ", zap.Duration("duration", time.Since(now)))
	return nil
}
func NewLogger(runId string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{
		fmt.Sprintf("./log_%s.json", runId),
	}
	cfg.ErrorOutputPaths = []string{
		fmt.Sprintf("./log_errors_%s.json", runId),
		"stderr",
	}
	l, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	l = l.With(zap.String("runId", runId))
	return l, nil
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	runId := strings.ReplaceAll(strings.ReplaceAll(time.Now().Format(time.DateTime), " ", "_"), ":", "-")
	logger, _ := NewLogger(runId)
	err := RunETL(ctx, runId, logger)
	if err != nil {
		logger.Error("Error while Running ETL", zap.Error(err))
	}
}

type DeliveryDBRecord struct {
	Id []byte `db:"uuid" sql:"pk"`
}

var CIO_HOSTS = []string{"shard-106462", "shard-111029", "shard-111991", "shard-114543", "shard-130145", "shard-131014", "shard-136108", "shard-154874", "shard-167241", "shard-24088", "shard-45402", "shard-63391", "shard-64420", "shard-66525", "shard-88438", "shard-92778", "shard-93295", "shard-a", "shard-b", "shard-c", "shard-d", "shard-e", "shard-f", "shard-g", "shard-h", "shard-i", "shard-j", "shard-k", "shard-l", "shard-lee-1", "shard-m", "shard-n", "shard-o", "shard-p", "shard-q", "shard-r", "shard-s", "shard-t", "shard-u"}

func cioDeliveryExtract() etl.ElementProcessor[etl.DBRecord[DeliveryDBRecord]] {
	var (
		host           = "0.0.0.0:9037"
		path           = "v1/rc/render_deliveries"
		requestBuilder = func(records []*etl.DBRecord[DeliveryDBRecord]) (*http.Request, error) {
			v := url.Values{}
			v.Add("env_id", strings.Split(records[0].DataBase, "production_env")[1])
			v.Add("include_liquid_response", "false")

			deliveryIds := make([]string, 0, len(records))
			for _, record := range records {
				deliveryIds = append(deliveryIds, base64.URLEncoding.EncodeToString(record.Record.Id))
			}
			payload := map[string]interface{}{
				"delivery_ids": deliveryIds,
			}
			payloadBytes, err := json.Marshal(payload)
			if err != nil {
				return nil, err
			}

			addr := &url.URL{
				Scheme:   "http",
				Host:     host,
				Path:     path,
				RawQuery: v.Encode(),
			}

			request, err := http.NewRequest("POST", addr.String(), bytes.NewBuffer(payloadBytes))
			if err != nil {
				return nil, err
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			return request, nil
		}
	)
	return etl.NewHTTPPostMapper[etl.DBRecord[DeliveryDBRecord]](requestBuilder)
}

func DummySource() etl.ElementSource[string] {

	var (
		deliveryIds = []string{
			"AZLlDsifKaLhPw4+EmsHDw==",
			"AZLlDsifnkY0W6cHQewHCQ==",
			"AZLlDsif+bunRUKXJDEoiA==",
			"AZLlKjgi3eP8JQcjOJ1NTg==",
			"AZLlLsvKq6AHnd0fRsNlNQ==",
			"AZLlQRtNd9Th0hEmVbVung==",
			"AZLlXJKh2U9Mv+SpPpJUuw==",
			"AZLlbxl8PpqGtu/pCcjZOg==",
			"AZLlc3nLH5xBMMZJT41O0Q==",
			"AZLlc7BqmxYcP69czRxQjA==",
		}
	)
	uuids := make([]*string, 0, len(deliveryIds))
	for _, id := range deliveryIds {
		uuids = append(uuids, &id)
	}
	return etl.NewSliceSource[string](uuids, 2)

}
