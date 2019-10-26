package pitr

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/tidb-binlog/proto/binlog"
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

	merge, err := NewMerge(nil, files, fileSize)
	assert.Assert(t, err == nil)

	err = merge.Map()
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

	merge.ddlHandle.ResetDB()
	sql := "create database test1; use test1; create table tb1 (a int);"
	mybin := &pb_binlog.Binlog{
		Tp:       pb_binlog.BinlogType_DDL,
		CommitTs: 100,
		DdlQuery: []byte(sql),
	}
	log, err := rewriteDDL(mybin, merge.ddlHandle)
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold(string(log.DdlQuery), "USE `test1`;CREATE TABLE `tb1` (`a` INT);"))
	merge.ddlHandle.ExecuteDDL(sql)

	sql = "drop database test1; create database test2; use test; show tables;"
	mybin = &pb_binlog.Binlog{
		Tp:       pb_binlog.BinlogType_DDL,
		CommitTs: 100,
		DdlQuery: []byte(sql),
	}
	log, err = rewriteDDL(mybin, merge.ddlHandle)
	fmt.Printf("%v\n", err)
	fmt.Printf("## %s\n", string(log.String()))
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold(string(log.DdlQuery), "DROP TABLE tb1;USE `test`;SHOW TABLES;"))
	merge.ddlHandle.ExecuteDDL(sql)

	merge.Close()
	merge.ddlHandle.Close()
	os.RemoveAll(dstPath + "/")
	os.RemoveAll(srcPath + "/")
}
