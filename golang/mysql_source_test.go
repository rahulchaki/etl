package etl

import (
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

type DBRecord_ struct {
	Id   []byte `db:"uuid" sql:"pk"`
	Data []byte `db:"data"`
}

func TestMySqlTableElementReader(t *testing.T) {
	t.Run("TestNewMySQLSource", func(t *testing.T) {
		var (
			database = "production_env39"
			table    = "delivs_2024_10"
		)
		conn, err := sqlx.Connect("mysql", "root@tcp(localhost:3306)/")
		if err != nil {
			t.Errorf("Could not connect to database: %v", err)
		}
		defer conn.Close()

		var countFromDB int
		err = conn.Get(&countFromDB, fmt.Sprintf("SELECT COUNT(*) as count FROM %s.%s", database, table))
		if err != nil {
			t.Errorf("Could not retrieve count from database: %v", err)
		}

		reader, err := NewMySqlTableElementReader[DBRecord_](database, table)
		if err != nil {
			panic(err)
		}
		defer reader.Close()
		var total int
		var offset interface{}
		for !reader.Done() {
			records, lastKey, err := reader.NextBatch(conn, 100)
			if err != nil {
				panic(err)
			}
			total += len(records)
			offset = lastKey
		}
		t.Logf("Total records read: %d, lastKey %v", total, offset)
		assert.Equal(t, countFromDB, total, "Count from DB does not match fre reader")
	})
}
