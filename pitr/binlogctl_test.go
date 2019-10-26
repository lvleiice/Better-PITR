package pitr

import (
	"os"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
	tb "github.com/pingcap/tipb/go-binlog"
	"gotest.tools/assert"
)

func genTestDDL(db, table, sql string, ts int64) *pb.Binlog {
	return &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte(sql),
		CommitTs: ts,
	}
}

func genTestDML(schema, table string, ts int64) *pb.Binlog {
	return &pb.Binlog{
		Tp: pb.BinlogType_DML,
		DmlData: &pb.DMLData{
			Events: generateDMLEvents(schema, table, ts),
		},
		CommitTs: ts,
	}
}

func generateDMLEvents(schema, table string, ts int64) []pb.Event {
	cols := generateColumns()
	return []pb.Event{
		{
			Tp:         pb.EventType_Insert,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[1]},
		}, {
			Tp:         pb.EventType_Delete,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[1], cols[1]},
		}, {
			Tp:         pb.EventType_Update,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[2]},
		},
	}
}

// generate columns for test
func generateColumns() [][]byte {
	allColBytes := make([][]byte, 0, 3)

	cols := []*pb.Column{
		{
			Name:      "a",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(1),
		}, {
			Name:      "b",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "varchar",
			Value:     encodeIntValue(2),
		}, {
			Name:         "c",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "varchar",
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

func encodeIntValue(value int64) []byte {
	b := make([]byte, 0, 5)
	// 3 means intFlag
	b = append(b, 3)
	b = codec.EncodeInt(b, value)
	return b
}

func genTestFiles(dirPath string) error {
	binlogger, err := binlogfile.OpenBinlogger(dirPath)
	if err != nil {
		return err
	}
	bin := genTestDDL("test", "t1", "use test;create table t1 (a int primary key, b int, c int)", 100)
	data, _ := bin.Marshal()
	binlogger.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDML("test", "t1", 200)
	data, _ = bin.Marshal()
	binlogger.WriteTail(&tb.Entity{Payload: data})

	bin = genTestDML("test", "t2", 200)
	data, _ = bin.Marshal()
	binlogger.WriteTail(&tb.Entity{Payload: data})

	binlogger.Close()

	return nil
}

func getFileSize(filename string) (int64, error) {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return 0, errors.Annotatef(err, "open file %s error", filename)
	}
	defer fd.Close()

	stat, err := fd.Stat()
	if err != nil {
		return 0, errors.Annotatef(err, "get file stat %s error", filename)
	}
	return stat.Size(), nil
}

func getTotalFileSize(filename []string) (int64, error) {
	var sum int64 = 0
	for _, v := range filename {
		n, err := getFileSize(v)
		if err != nil {
			return sum, err
		}
		sum += n
	}
	return sum, nil
}

func TestMyBinlogger(t *testing.T) {
	//generate binlogs
	src_path := "./binlog"
	os.RemoveAll(src_path + "/")

	genTestFiles(src_path)

	dst_path := "./testcase"
	os.RemoveAll(dst_path + "/")
	b, err := OpenMyBinlogger(dst_path)
	assert.Assert(t, err == nil)

	bin := genTestDDL("test", "t1", "use test;create table t1 (a int primary key, b int, c int)", 100)
	data, _ := bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})
	bin = genTestDML("test", "t1", 200)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})
	bin = genTestDML("test", "t2", 200)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	b.Close()

	src_files, err := searchFiles(src_path)
	assert.Assert(t, err == nil)
	dst_files, err := searchFiles(dst_path)
	assert.Assert(t, err == nil)

	assert.Assert(t, len(src_files) == len(dst_files))

	src_num, err := getTotalFileSize(src_files)
	assert.Assert(t, err == nil)
	dst_num, err := getTotalFileSize(dst_files)

	assert.Assert(t, src_num == dst_num)

	os.RemoveAll(src_path + "/")
	os.RemoveAll(dst_path + "/")

}

func TestRoate(t *testing.T) {
	dst_path := "./testroate"

	os.RemoveAll(dst_path + "/")

	b, err := OpenMyBinlogger(dst_path)
	assert.Assert(t, err == nil)

	bin := genTestDDL("test", "t1", "use test;create table t1 (a int primary key, b int, c int)", 100)
	data, _ := bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})
	b.ManualRotate()
	bin = genTestDML("test", "t1", 200)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})
	b.ManualRotate()
	bin = genTestDML("test", "t2", 200)
	data, _ = bin.Marshal()
	b.WriteTail(&tb.Entity{Payload: data})

	b.Close()

	files, err := searchFiles(dst_path)
	assert.Assert(t, len(files) == 3)
	os.RemoveAll(dst_path + "/")
}
