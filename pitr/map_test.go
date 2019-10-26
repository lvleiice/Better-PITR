package pitr

import (
	"os"
	"testing"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"gotest.tools/assert"
)

func TestPbFile(t *testing.T) {
	dirPath := "./test_pbfile"
	os.RemoveAll(dirPath + "/")

	schema := "db1"
	table := "tb1"

	f, err := NewPbFile(dirPath, schema, table, 2)
	assert.Assert(t, err == nil)

	cols := generateColumns()
	ev := pb.Event{
		Tp:         pb.EventType_Insert,
		SchemaName: &schema,
		TableName:  &table,
		Row:        [][]byte{cols[0], cols[1]},
	}
	f.AddDMLEvent(ev, 35, string(cols[0]))

	f.AddDDLEvent(&pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create table tx (a int)"),
		CommitTs: 36,
	})

	ev = pb.Event{
		Tp:         pb.EventType_Insert,
		SchemaName: &schema,
		TableName:  &table,
		Row:        [][]byte{cols[1], cols[0]},
	}

	f.AddDMLEvent(ev, 37, string(cols[1]))

	f.Close()

	files, err := searchFiles(dirPath + "/" + "db1_tb1")
	assert.Assert(t, err == nil)

	files, _, err = filterFiles(files, 0, 1000000000)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(files) == 3)

	os.RemoveAll(dirPath + "/")

}

func TestPbFileDDL(t *testing.T) {
	dirPath := "./test_pbfile_ddl"
	os.RemoveAll(dirPath + "/")

	schema := "db1"
	table := "tb1"

	f, err := NewPbFile(dirPath, schema, table, 2)
	assert.Assert(t, err == nil)

	f.AddDDLEvent(&pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create table tx (a int)"),
		CommitTs: 36,
	})

	f.Close()

	files, err := searchFiles(dirPath + "/" + "db1_tb1")
	assert.Assert(t, err == nil)

	files, _, err = filterFiles(files, 0, 40)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(files) == 1)

	os.RemoveAll(dirPath + "/")
}
