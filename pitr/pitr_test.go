package pitr

import (
	"fmt"
	"os"
	"testing"

	tb "github.com/pingcap/tipb/go-binlog"
	"gotest.tools/assert"
)

func TestNewPitr(t *testing.T) {
	testDir := "/tmp/pitr_test/input"
	generateTestData(t, testDir)
	pitr, err := New(&Config{
		Dir:       testDir,
		OutputDir: "/tmp/pitr_test/output",
	})
	assert.Assert(t, pitr != nil)
	assert.Assert(t, err == nil)

	err = pitr.Process()
	fmt.Println(err)
	assert.Assert(t, err == nil)
}

func generateTestData(t *testing.T, dir string) {
	os.RemoveAll(dir + "/")
	os.RemoveAll(defaultTiDBDir)
	os.RemoveAll(defaultTempDir)

	//generate files

	b, err := OpenMyBinlogger(dir)
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
}
