package etl

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type DBRecord[T any] struct {
	DataBase string
	Table    string
	Id       any
	Record   *T
}

type MySQLSource[T any] struct {
	shards []ElementShard[DBRecord[T]]
}

func NewMySQLSource[T any](hosts []string, user, dbMatching, table string) (ElementSource[DBRecord[T]], error) {
	var shards []ElementShard[DBRecord[T]]
	for _, host := range hosts {
		shard, err := NewMySQLShard[T](host, host, user, dbMatching, table)
		if err != nil {
			return nil, err
		}
		shards = append(shards, shard)
	}

	return &MySQLSource[T]{
		shards: shards,
	}, nil
}

func (s *MySQLSource[T]) Id() string {
	return "mysql"
}

func (s *MySQLSource[T]) Shards() ([]ElementShard[DBRecord[T]], error) {
	return s.shards, nil
}

type MySQLShard[T any] struct {
	shard string

	connUrl    string
	partitions []ElementPartition[DBRecord[T]]
}

func NewMySQLShard[T any](shard, host, user, dbMatching, table string) (ElementShard[DBRecord[T]], error) {

	conn, err := sqlx.Connect("mysql", fmt.Sprintf("%s@tcp(%s:3306)/", user, host))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	databases, err := scanDatabases(conn, dbMatching)
	if err != nil {
		return nil, err
	}

	var partitions []ElementPartition[DBRecord[T]]
	for _, database := range databases {
		partition, err := NewMySqlTableElementReader[T](conn, database, table)
		if err != nil {
			continue
			//return nil, err
		}
		partitions = append(partitions, partition)
	}
	connUrl := fmt.Sprintf("%s@tcp(%s:3306)/", user, host)
	return &MySQLShard[T]{
		shard:      shard,
		connUrl:    connUrl,
		partitions: partitions,
	}, nil
}

func (s *MySQLShard[T]) Id() string {
	return s.shard
}

func (s *MySQLShard[T]) NewResource() (Closeable, error) {
	conn, err := sqlx.Connect("mysql", s.connUrl)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (s *MySQLShard[T]) Partitions() ([]ElementPartition[DBRecord[T]], error) {
	return s.partitions, nil
}

type MySqlTableElementReader[T any] struct {
	database string
	table    string

	projection *sqlProjection
	extractId  func(record *T) interface{}

	isDone  bool
	lastKey interface{}
}

type sqlProjection struct {
	pkIndex  int
	pkColumn string
	fields   []string
}

func extractPkColumn[T any]() *sqlProjection {
	var record T
	st := reflect.TypeOf(record)
	var (
		fields []string
		pkCol  string
		pkIdx  int
	)

	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		if dbTag, ok := field.Tag.Lookup("db"); ok {
			dbTags := strings.Split(dbTag, ",")
			if len(dbTags) <= 0 {
				continue
			}
			fields = append(fields, dbTags[0])
			if strings.Contains(field.Tag.Get("sql"), "pk") {
				pkCol = dbTags[0]
				pkIdx = i
			}
		}
	}
	return &sqlProjection{
		pkColumn: pkCol,
		pkIndex:  pkIdx,
		fields:   fields,
	}
}

func NewMySqlTableElementReader[T any](conn *sqlx.DB, database string, table string) (ElementPartition[DBRecord[T]], error) {
	projection := extractPkColumn[T]()

	extractId := func(record *T) any {
		value := reflect.ValueOf(record).Elem()
		return value.FieldByIndex([]int{projection.pkIndex}).Interface()
	}
	row, err := conn.Queryx(fmt.Sprintf("SELECT %s as count FROM %s.%s LIMIT 1", projection.pkColumn, database, table))
	if err != nil {
		return nil, err
	}
	if !row.Next() {
		return nil, fmt.Errorf("Table %s.%s is Empty", database, table)
	}

	return &MySqlTableElementReader[T]{
		database: database,
		table:    table,

		projection: projection,
		extractId:  extractId,

		isDone:  false,
		lastKey: nil,
	}, nil
}

func (r *MySqlTableElementReader[T]) Id() string {
	return fmt.Sprintf("%s.%s", r.database, r.table)
}

func (r *MySqlTableElementReader[T]) Done() bool {
	return r.isDone
}

func (r *MySqlTableElementReader[T]) NextBatch(resource interface{}, batchSize int) ([]*DBRecord[T], interface{}, error) {
	if r.isDone {
		return nil, r.lastKey, nil
	}
	conn := resource.(*sqlx.DB)
	records, err := readMySQlTableInBatch[T](conn, r.database, r.table, r.projection.pkColumn, r.projection.fields, batchSize, r.lastKey)
	if err != nil {
		return nil, r.lastKey, err
	}

	var dbRecords []*DBRecord[T]
	for _, record := range records {
		dbRecords = append(dbRecords, &DBRecord[T]{
			DataBase: r.database,
			Table:    r.table,
			Id:       r.extractId(record),
			Record:   record,
		})
	}

	if len(dbRecords) > 0 {
		r.lastKey = dbRecords[len(dbRecords)-1].Id
	}
	if len(dbRecords) < batchSize {
		r.isDone = true
		return dbRecords, r.lastKey, r.Close()
	}
	return dbRecords, r.lastKey, nil
}

func (r *MySqlTableElementReader[T]) Close() error {
	return nil
}

func scanDatabases(conn *sqlx.DB, dbMatching string) ([]string, error) {
	var databases []string
	err := conn.Select(&databases, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	dbPattern, err := regexp.Compile(dbMatching)
	if err != nil {
		return nil, err
	}
	var filtered []string
	for _, db := range databases {
		if dbPattern.MatchString(db) {
			filtered = append(filtered, db)
		}
	}
	return filtered, nil
}

func readMySQlTableInBatch[T any](
	conn *sqlx.DB,
	database string,
	table string,
	pkCol string,
	fields []string,
	limit int,
	lastKey interface{},
) ([]*T, error) {

	var (
		records []*T
		err     error
	)

	if lastKey == nil {
		query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT %d ", strings.Join(fields, ","), database, table, limit)
		err = conn.Select(&records, query)

	} else {
		query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s > ? LIMIT %d ", strings.Join(fields, ","), database, table, pkCol, limit)
		err = conn.Select(&records, query, lastKey)
	}
	if err != nil {
		return nil, err
	}
	return records, nil
}
