package pitr

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/tidb-binlog/proto/binlog"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
	"gotest.tools/assert"
)

func TestMapFunc1(t *testing.T) {
	dstPath := "./test_map"
	srcPath := "./maptest"
	os.RemoveAll(dstPath + "/")
	os.RemoveAll(srcPath + "/")
	os.RemoveAll(defaultTiDBDir)
	os.RemoveAll(defaultTempDir)

	//generate files

	b, err := OpenMyBinlogger(srcPath)
	assert.Assert(t, err == nil)

	bin := genTestDDL("test", "tb1", "use test;create table tb1 (a int primary key, b int, c int)", 100)
	data, _ := bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDML("test", "tb1", 200)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDDL("test", "tb1", "use test; drop table tb1", 201)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDDL("test", "tb2", "use test; create table tb2 (a int primary key, b int, c int)", 203)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})
	bin = genTestDML("test", "tb2", 204)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDML("test", "tb2", 205)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	b.Close()

	files, err := searchFiles(srcPath)
	assert.Assert(t, err == nil)

	files, fileSize, err := filterFiles(files, 0, 300)
	assert.Assert(t, err == nil)

	merge, err := NewMerge(files, fileSize)
	assert.Assert(t, err == nil)

	_, err = merge.Map(0, math.MaxInt64)
	assert.Assert(t, err == nil)

	tb1, err := searchFiles(merge.tempDir + "/" + "test_tb1")
	assert.Assert(t, err == nil)
	tb1f, _, err := filterFiles(tb1, 0, 300)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(tb1f) == 3)

	tb2, err := searchFiles(merge.tempDir + "/" + "test_tb2")
	assert.Assert(t, err == nil)
	tb2f, _, err := filterFiles(tb2, 0, 300)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(tb2f) == 2)

	err = merge.Reduce()
	assert.Assert(t, err == nil)

	ddlHandle.ResetDB()
	createDBSQL := "create database test1;"
	err = ddlHandle.ExecuteDDL("test1", createDBSQL)
	assert.Assert(t, err == nil)

	sql := "use test1; create table tb1 (a int);"
	mybin := &pb_binlog.Binlog{
		Tp:       pb_binlog.BinlogType_DDL,
		CommitTs: 100,
		DdlQuery: []byte(sql),
	}
	log, err := rewriteDDL(mybin)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold(string(log.DdlQuery), "USE `test1`;CREATE TABLE `tb1` (`a` INT);"))
	err = ddlHandle.ExecuteDDL("test1", sql)
	assert.Assert(t, err == nil)

	sql = "drop database test1; create database test2; use test; show tables;"
	mybin = &pb_binlog.Binlog{
		Tp:       pb_binlog.BinlogType_DDL,
		CommitTs: 100,
		DdlQuery: []byte(sql),
	}
	log, err = rewriteDDL(mybin)
	fmt.Printf("%v\n", err)
	fmt.Printf("## %s\n", string(log.String()))
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold(string(log.DdlQuery), "DROP TABLE tb1;USE `test`;SHOW TABLES;"))
	ddlHandle.ExecuteDDL("test1", sql)

	merge.Close(false)
	ddlHandle.Close()
	os.RemoveAll(dstPath + "/")
	os.RemoveAll(srcPath + "/")
	os.RemoveAll(defaultOutputDir)
}

func TestRewriteDML(t *testing.T) {
	ev, err := generateUpdateEvent("test1", "tb1", 1024)
	assert.Assert(t, err == nil)
	evs, err := rewriteDML(ev)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(evs) == 2)
	assert.Assert(t, evs[0].Tp == pb.EventType_Delete)
	assert.Assert(t, evs[1].Tp == pb.EventType_Insert)
}

func generateUpdateEvent(schema, table string, ts int64) (*pb.Event, error) {
	col1, err := generateUpdateColumn(1, 2)
	if err != nil {
		return nil, err
	}
	col2, err := generateUpdateColumn(2, 3)
	if err != nil {
		return nil, err
	}
	return &pb.Event{
		Tp:         pb.EventType_Update,
		SchemaName: &schema,
		TableName:  &table,
		Row:        [][]byte{col1, col2},
	}, nil
}

// generate columns for test
func generateUpdateColumn(before, after int64) ([]byte, error) {
	col := &pb.Column{
		Name:         "a",
		Tp:           []byte{mysql.TypeInt24},
		MysqlType:    "int",
		Value:        encodeIntValue(before),
		ChangedValue: encodeIntValue(after),
	}
	bt, err := col.Marshal()
	if err != nil {
		return nil, err
	}
	return bt, nil
}
