package pitr

import (
	"gotest.tools/assert"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/parser/mysql"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

func TestGetHashKey(t *testing.T) {
	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle(nil)
	assert.Assert(t, err == nil)
	ddl.ResetDB()

	err = ddl.createMapTable()
	assert.Assert(t, err == nil)

	schema := "test5"
	table := "tb1"
	//test primary/unique key
	evs := genTestUpdateEvent("test5", "tb1")
	err = ddl.ExecuteDDL("create database test5;")
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("use test5; create table tb1 (a int unique, b int)")
	assert.Assert(t, err == nil)
	key, err := getHashKey(schema, table, evs[0], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb1|1|", key))

	key, err = getHashKey(schema, table, evs[1], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb1|1|", key))

	key, err = getHashKey(schema, table, evs[2], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb1|1|", key))

	//test non primary/unique key
	table = "tb2"
	evs = genTestUpdateEvent("test5", "tb2")
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("use test5; create table tb2 (a int, b int)")
	assert.Assert(t, err == nil)
	key, err = getHashKey(schema, table, evs[0], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb2|1|1|", key))

	key, err = getHashKey(schema, table, evs[1], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb2|2|2|", key))

	key, err = getHashKey(schema, table, evs[2], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb2|3|3|", key))

	//test insert
	table = "tb3"
	evs = genTestInsertEvent("test5", "tb3")
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("use test5; create table tb3 (a int primary key, b int)")
	assert.Assert(t, err == nil)
	key, err = getHashKey(schema, table, evs[0], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb3|1|", key))

	key, err = getHashKey(schema, table, evs[1], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb3|2|", key))

	key, err = getHashKey(schema, table, evs[2], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb3|3|", key))

	table = "tb4"
	evs = genTestInsertEvent("test5", "tb4")
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("use test5; create table tb4 (a int, b int)")
	assert.Assert(t, err == nil)
	key, err = getHashKey(schema, table, evs[0], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb4|1|1|", key))

	key, err = getHashKey(schema, table, evs[1], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb4|2|2|", key))

	key, err = getHashKey(schema, table, evs[2], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb4|3|3|", key))

	//test delete
	table = "tb5"
	evs = genTestInsertEvent("test5", "tb5")
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("use test5; create table tb5 (a int, b int)")
	assert.Assert(t, err == nil)
	key, err = getHashKey(schema, table, evs[0], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb5|1|1|", key))

	key, err = getHashKey(schema, table, evs[1], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb5|2|2|", key))

	key, err = getHashKey(schema, table, evs[2], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb5|3|3|", key))

	table = "tb6"
	evs = genTestInsertEvent("test5", "tb6")
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("use test5; create table tb6 (a int primary key, b int)")
	assert.Assert(t, err == nil)
	key, err = getHashKey(schema, table, evs[0], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb6|1|", key))

	key, err = getHashKey(schema, table, evs[1], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb6|2|", key))

	key, err = getHashKey(schema, table, evs[2], ddl)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("test5|tb6|3|", key))
}

func genTestUpdateColumn() [][]byte {
	allColBytes := make([][]byte, 0, 3)
	cols := []*pb.Column{
		{
			Name:         "a",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(1),
			ChangedValue: encodeIntValue(2),
		},
		{
			Name:         "a",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(2),
			ChangedValue: encodeIntValue(3),
		},
		{
			Name:         "a",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(3),
			ChangedValue: encodeIntValue(4),
		},
		{
			Name:         "b",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(1),
			ChangedValue: encodeIntValue(2),
		},
		{
			Name:         "b",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(2),
			ChangedValue: encodeIntValue(3),
		},
		{
			Name:         "b",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(3),
			ChangedValue: encodeIntValue(4),
		},
	}
	for _, col := range cols {
		colBytes, _ := col.Marshal()

		allColBytes = append(allColBytes, colBytes)
	}

	return allColBytes
}

func genTestUpdateEvent(schema, table string) []pb.Event {
	cols := genTestUpdateColumn()
	return []pb.Event{
		{
			Tp:         pb.EventType_Update,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[3]},
		}, {
			Tp:         pb.EventType_Update,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[1], cols[4]},
		},
		{
			Tp:         pb.EventType_Update,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[2], cols[5]},
		},
	}
}

func genTestCommonColumn() [][]byte {
	allColBytes := make([][]byte, 0, 3)
	cols := []*pb.Column{
		{
			Name:      "a",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(1),
		},
		{
			Name:      "a",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(2),
		},
		{
			Name:      "a",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(3),
		},
		{
			Name:      "b",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(1),
		},
		{
			Name:      "b",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(2),
		},
		{
			Name:      "b",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(3),
		},
	}
	for _, col := range cols {
		colBytes, _ := col.Marshal()

		allColBytes = append(allColBytes, colBytes)
	}

	return allColBytes
}

func genTestInsertEvent(schema, table string) []pb.Event {
	cols := genTestCommonColumn()
	return []pb.Event{
		{
			Tp:         pb.EventType_Insert,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[3]},
		}, {
			Tp:         pb.EventType_Insert,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[1], cols[4]},
		},
		{
			Tp:         pb.EventType_Insert,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[2], cols[5]},
		},
	}
}

func genTestDeleteEvent(schema, table string) []pb.Event {
	cols := genTestCommonColumn()
	return []pb.Event{
		{
			Tp:         pb.EventType_Delete,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[3]},
		}, {
			Tp:         pb.EventType_Delete,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[1], cols[4]},
		},
		{
			Tp:         pb.EventType_Delete,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[2], cols[5]},
		},
	}
}
