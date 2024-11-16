module etl

go 1.23.2

replace github.com/customerio/services => ../../services

require (
	github.com/customerio/services v0.0.0-20220119193552-3b3b3b3b3b3b
	github.com/go-sql-driver/mysql v1.8.1
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.9.0
	go.uber.org/zap v1.27.0
	golang.org/x/sync v0.9.0
)

require (
	cloud.google.com/go v0.115.0 // indirect
	cloud.google.com/go/auth v0.6.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.2 // indirect
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	cloud.google.com/go/iam v1.1.8 // indirect
	cloud.google.com/go/secretmanager v1.13.1 // indirect
	cloud.google.com/go/storage v1.42.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.3.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.1.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.0.0 // indirect
	github.com/Azure/azure-storage-blob-go v0.13.0 // indirect
	github.com/DataDog/zstd v1.5.5 // indirect
	github.com/Pallinder/go-randomdata v1.2.0 // indirect
	github.com/RoaringBitmap/roaring v1.2.1 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr v1.4.10 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200601151325-b2287a20f230 // indirect
	github.com/apache/thrift v0.17.0 // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20240412182139-38384edc16db // indirect
	github.com/aws/aws-sdk-go v1.38.24 // indirect
	github.com/aws/aws-sdk-go-v2 v1.2.0 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.1.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.0.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.0.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.1.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.2.0 // indirect
	github.com/aws/smithy-go v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-simplejson v0.5.1-0.20200416141419-39a59b1b2866 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b // indirect
	github.com/btubbs/datetime v0.1.1 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/certifi/gocertifi v0.0.0-20210507211836-431795d63e8d // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.9.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/pebble v0.0.0-20230712164400-52cfb819e866 // indirect
	github.com/cockroachdb/redact v1.1.4 // indirect
	github.com/coreos/go-oidc v2.2.1+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/customerio/bitmaps v1.0.1 // indirect
	github.com/customerio/clock v1.0.0 // indirect
	github.com/customerio/esdb v1.0.4 // indirect
	github.com/customerio/gaia/discovery v0.0.0-20241022174422-9defacbd5670 // indirect
	github.com/customerio/gaia/kmq v0.0.0-20241016064931-bfd765765a26 // indirect
	github.com/customerio/gaia/kmq/service v0.0.0-20241016064931-bfd765765a26 // indirect
	github.com/customerio/gaia/libraries v0.0.0-20241106191645-feaa8a8a8f6f // indirect
	github.com/customerio/gaia/mq v0.0.0-20241016064917-7696320cd4ba // indirect
	github.com/customerio/go-approval-tests v0.0.0-20240709125329-edff8d031448 // indirect
	github.com/customerio/go-customerio/v3 v3.3.0 // indirect
	github.com/customerio/go-fdb-record-layer v0.0.0-20240416133350-a527653fec6d // indirect
	github.com/customerio/goconfig v1.0.0 // indirect
	github.com/customerio/httprouter v1.2.0 // indirect
	github.com/customerio/hydra/billing v0.0.0-20241107214804-ce41a3a0094e // indirect
	github.com/customerio/hydra/hydra_integration v0.0.0-20241107214914-c4e04bf67a8d // indirect
	github.com/customerio/hydra/libraries v0.0.0-20241107214914-c4e04bf67a8d // indirect
	github.com/customerio/hydra/permissions v0.0.0-20240819180604-c0ec794a15b4 // indirect
	github.com/customerio/raven-go v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/emersion/go-msgauth v0.6.8 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/facebookgo/limitgroup v0.0.0-20150612190941-6abd8d71ec01 // indirect
	github.com/facebookgo/muster v0.0.0-20150708232844-fd3d7953fd52 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/form3tech-oss/jwt-go v3.2.2+incompatible // indirect
	github.com/gammazero/deque v0.2.0 // indirect
	github.com/gammazero/workerpool v1.1.3 // indirect
	github.com/garyburd/redigo v1.6.2 // indirect
	github.com/getsentry/sentry-go v0.21.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v5 v5.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/cel-go v0.13.0 // indirect
	github.com/google/flatbuffers v23.5.26+incompatible // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.5 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/honeycombio/libhoney-go v1.15.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mParticle/mparticle-go-sdk v1.1.1 // indirect
	github.com/mailgun/mailgun-go/v4 v4.8.2 // indirect
	github.com/mailproto/textplain v0.2.9 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pelletier/go-toml/v2 v2.0.7 // indirect
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4 // indirect
	github.com/planetscale/vtprotobuf v0.5.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pquerna/cachecontrol v0.1.0 // indirect
	github.com/prometheus/client_golang v1.20.2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/segmentio/analytics-go v3.1.0+incompatible // indirect
	github.com/segmentio/backo-go v1.0.0 // indirect
	github.com/sendgrid/rest v2.6.3+incompatible // indirect
	github.com/sendgrid/sendgrid-go v3.9.0+incompatible // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/slack-go/slack v0.12.2 // indirect
	github.com/snowflakedb/gosnowflake v1.4.2 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/stripe/stripe-go v70.15.0+incompatible // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	github.com/umpc/go-sortedmap v0.0.0-20180422175548-64ab94c482f4 // indirect
	github.com/viney-shih/go-lock v1.1.2 // indirect
	github.com/vmihailenco/msgpack/v4 v4.3.12 // indirect
	github.com/vmihailenco/tagparser v0.1.1 // indirect
	github.com/xitongsys/parquet-go v1.5.4 // indirect
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/zeebo/errs v1.3.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.9 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/oauth2 v0.21.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	golang.org/x/time v0.7.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/api v0.186.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240617180043-68d350f18fd4 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240617180043-68d350f18fd4 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240617180043-68d350f18fd4 // indirect
	google.golang.org/grpc v1.64.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/alexcesaro/statsd.v2 v2.0.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	rogchap.com/v8go v0.8.0 // indirect
	storj.io/drpc v0.0.33 // indirect
)
