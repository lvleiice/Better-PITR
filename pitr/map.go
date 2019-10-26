package pitr

import (
	"hash/crc32"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
)

const (
	Max_Event_Num = 1024
)

type PBFile struct {
	schema    string
	table     string
	num       int
	binlogger *myBinlogger
	dml       map[int]*pb.Binlog
	ddl       []*pb.Binlog
}

func NewPbFile(dir, schema, table string, num int) (*PBFile, error) {
	b, err := OpenMyBinlogger(dir + "/" + schema + "_" + table)
	if err != nil {
		return nil, err
	}
	return &PBFile{
		schema:    schema,
		table:     table,
		num:       num,
		binlogger: b,
		ddl:       nil,
		dml:       make(map[int]*pb.Binlog, num),
	}, nil
}

func (f *PBFile) getHashCode(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % f.num
}

func (f *PBFile) AddDMLEvent(ev pb.Event, commitTS int64, key string) error {
	f.flushDDL(true)
	h := f.getHashCode(key)
	if f.dml[h] == nil {
		f.dml[h] = &pb.Binlog{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: make([]pb.Event, 0, Max_Event_Num),
			},
		}
	}
	f.dml[h].DmlData.Events = append(f.dml[h].DmlData.Events, ev)
	f.dml[h].CommitTs = mathutil.MaxInt64Val(f.dml[h].CommitTs, commitTS)
	if len(f.dml[h].DmlData.Events) >= Max_Event_Num {
		return f.flushDML(h, true)
	}
	return nil
}

func (f *PBFile) AddDDLEvent(binlog *pb.Binlog) error {
	dml := f.dml
	for n := range dml {
		f.flushDML(n, true)
	}
	f.ddl = append(f.ddl, binlog)
	if len(f.ddl) >= Max_Event_Num {
		return f.flushDDL(true)
	}
	return nil
}
func (f *PBFile) flushDML(n int, b bool) error {
	if len(f.dml[n].DmlData.Events) == 0 {
		return nil
	}
	var sum int64
	data, err := f.dml[n].Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	sum, err = f.binlogger.WriteTail(&tb.Entity{Payload: data})
	if err != nil {
		return errors.Trace(err)
	}
	f.dml[n] = &pb.Binlog{
		Tp:       pb.BinlogType_DML,
		CommitTs: 0,
		DmlData: &pb.DMLData{
			Events: make([]pb.Event, 0, Max_Event_Num),
		}}
	if sum > 0 && b {
		f.binlogger.ManualRotate()
	}
	return nil
}

func (f *PBFile) flushDDL(b bool) error {
	if len(f.ddl) == 0 {
		return nil
	}
	var sum int64
	var n int64
	for _, v := range f.ddl {
		data, err := v.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		n, err = f.binlogger.WriteTail(&tb.Entity{Payload: data})
		sum += n
	}
	f.ddl = nil
	if sum > 0 && b {
		f.binlogger.ManualRotate()
	}
	return nil
}

func (f *PBFile) Roate() error {
	return f.binlogger.ManualRotate()
}

func (f *PBFile) Close() {
	for n, v := range f.dml {
		if v != nil && len(v.DmlData.Events) > 0 {
			f.flushDML(n, false)
		}
	}
	f.flushDDL(false)

	if f.binlogger != nil {
		f.binlogger.Close()
	}
}
