package pitr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

const (
	maxMemorySize  int64 = 2 * 1024 * 1024 * 1024 // 2G
	beforeImageRow byte  = 0x1
	afterImageRow  byte  = 0x2
)

var (
	defaultTempDir   string = "./temp"
	defaultOutputDir string = "./new_binlog"

	// used for handle ddl, and update table info
	ddlHandle *DDLHandle
)

// Merge used to merge same keys binlog into one
type Merge struct {
	// tempDir used to save splited binlog file
	tempDir string

	// outputDir used to save merged binlog file
	outputDir string

	// which binlog file need merge
	binlogFiles []string

	// memory maybe not enough, need split all binlog files into multiple temp files
	splitNum int

	wg sync.WaitGroup
}

// NewMerge returns a new Merge
func NewMerge(historyDDLs []*model.Job, binlogFiles []string, allFileSize int64) (*Merge, error) {
	err := os.Mkdir(defaultTempDir, 0700)
	if err != nil {
		return nil, err
	}

	ddlHandle, err = NewDDLHandle(historyDDLs)
	if err != nil {
		return nil, err
	}

	var snum int
	if allFileSize <= maxMemorySize {
		snum = 1
	} else {
		snum = int(allFileSize / maxMemorySize)
	}
	return &Merge{
		tempDir:     defaultTempDir,
		outputDir:   defaultOutputDir,
		binlogFiles: binlogFiles,
		splitNum:    snum,
	}, nil
}

// Map split binlog into multiple files
func (m *Merge) Map() error {
	fileMap := make(map[string]*PBFile)
	log.Info("map", zap.Strings("files", m.binlogFiles))

	for _, bFile := range m.binlogFiles {
		f, err := os.OpenFile(bFile, os.O_RDONLY, 0600)
		if err != nil {
			return errors.Annotatef(err, "open file %s error", bFile)
		}
		reader := bufio.NewReader(f)
		for {
			var key, schema, table string
			var pf *PBFile
			binlog, _, err := Decode(reader)
			if err != nil {
				if errors.Cause(err) == io.EOF {
					break
				} else {
					return err
				}
			}

			switch binlog.Tp {

			case pb.BinlogType_DML:
				dml := binlog.DmlData
				if dml == nil {
					return errors.New("dml binlog's data can't be empty")
				}
				for _, event := range dml.Events {
					schema = event.GetSchemaName()
					table = event.GetTableName()
					key = fmt.Sprintf("%s_%s", schema, table)
					if fileMap[key] == nil {
						pf, err = NewPbFile(m.tempDir, schema, table, m.splitNum)
						if err != nil {
							return errors.Trace(err)
						}
						fileMap[key] = pf
					} else {
						pf = fileMap[key]
					}
					var evs []*pb.Event
					evs, err = rewriteDML(&event)
					if err != nil {
						return err
					}
					for _, v := range evs {
						var hk string
						hk, err = getHashKey(schema, table, v)
						if err != nil {
							return err
						}
						pf.AddDMLEvent(event, binlog.CommitTs, hk)
					}
				}
			case pb.BinlogType_DDL:
				schema, table, err = parserSchemaTableFromDDL(string(binlog.DdlQuery))
				if err != nil {
					return errors.Trace(err)
				}
				if len(schema) == 0 {
					return errors.New("DDL has no schema info.")
				}
				key = fmt.Sprintf("%s_%s", schema, table)
				if fileMap[key] == nil {
					pf, err = NewPbFile(m.tempDir, schema, table, m.splitNum)
					if err != nil {
						return errors.Trace(err)
					}
					fileMap[key] = pf
				} else {
					pf = fileMap[key]
				}
				var rebin *pb.Binlog
				rebin, err = rewriteDDL(binlog)
				if err != nil {
					return err
				}
				err = ddlHandle.ExecuteDDL(string(binlog.GetDdlQuery()))
				if err != nil {
					return err
				}
				pf.AddDDLEvent(rebin)
			default:
				panic("unreachable")

			}
		}

	}
	for _, v := range fileMap {
		v.Close()
	}

	ddlHandle.ResetDB()
	return nil
}

// Reduce merge same keys binlog into one, and output to file
// every file only contain one table's binlog, just like:
// - output
//   - schema1_table1
//   _ schema1_table2
//   - schema2_table1
//   - schema2_table2
func (m *Merge) Reduce() error {
	subDirs, err := binlogfile.ReadDir(m.tempDir)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("", zap.Strings("sub dirs", subDirs))

	resultCh := make(chan error, len(subDirs))

	for _, dir := range subDirs {
		tableMerge, err := NewTableMerge(path.Join(m.tempDir, dir), path.Join(defaultOutputDir, dir))
		if err != nil {
			return errors.Trace(err)
		}

		go tableMerge.Process(resultCh)
	}

	successNum := 0
	for {
		select {
		case err := <-resultCh:
			if err != nil {
				return err
			}

			successNum++
			if successNum == len(subDirs) {
				return nil
			}
		}
	}

	return err
}

func (m *Merge) Close(reserve bool) {
	if !reserve {
		if err := os.RemoveAll(m.tempDir); err != nil {
			log.Warn("remove temp dir", zap.String("dir", m.tempDir), zap.Error(err))
		}
	}
	ddlHandle.Close()
}

type TableMerge struct {
	inputDir  string
	outputDir string

	keyEvent map[string]*Event

	binlogger binlogfile.Binlogger

	maxCommitTS int64
}

func NewTableMerge(inputDir, outputDir string) (*TableMerge, error) {
	binlogger, err := binlogfile.OpenBinlogger(outputDir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &TableMerge{
		inputDir:  inputDir,
		outputDir: outputDir,
		keyEvent:  make(map[string]*Event),
		binlogger: binlogger,
	}, nil
}

func (tm *TableMerge) Process(resultCh chan error) {
	fNames, err := binlogfile.ReadDir(tm.inputDir)
	if err != nil {
		resultCh <- errors.Trace(err)
	}
	log.Info("reduce", zap.String("dir", tm.inputDir), zap.Strings("files", fNames))

	for _, fName := range fNames {
		binlogCh, errCh := tm.read(path.Join(tm.inputDir, fName))

	Loop:
		for {
			select {
			case binlog, ok := <-binlogCh:
				if ok {
					err := tm.analyzeBinlog(binlog)
					if err != nil {
						resultCh <- errors.Trace(err)
					}
					tm.maxCommitTS = binlog.CommitTs
				} else {
					break Loop
				}
			case err := <-errCh:
				resultCh <- errors.Trace(err)
			}
		}
	}

	err = tm.FlushDMLBinlog(tm.maxCommitTS)
	if err != nil {
		resultCh <- errors.Trace(err)
	}

	log.Info("reduce finished", zap.String("dir", tm.inputDir))
	resultCh <- nil
}

// FlushDMLBinlog merge some events to one binlog, and then write to file
func (tm *TableMerge) FlushDMLBinlog(commitTS int64) error {
	binlog := newDMLBinlog(commitTS)
	i := 0
	for _, row := range tm.keyEvent {
		i++
		r := make([][]byte, 0, 10)
		for _, c := range row.cols {
			data, err := c.Marshal()
			if err != nil {
				return err
			}
			r = append(r, data)
		}

		log.Debug("generate new event", zap.String("event", fmt.Sprintf("%v", row)))
		newEvent := pb.Event{
			SchemaName: &row.schema,
			TableName:  &row.table,
			Tp:         row.eventType,
			Row:        r,
		}
		binlog.DmlData.Events = append(binlog.DmlData.Events, newEvent)

		// every binlog contain 1000 rows as default
		if i%1000 == 0 {
			err := tm.writeBinlog(binlog)
			if err != nil {
				return err
			}
			binlog = newDMLBinlog(commitTS)
		}
	}

	if len(binlog.DmlData.Events) != 0 {
		err := tm.writeBinlog(binlog)
		if err != nil {
			return err
		}
	}

	// all event have already flush to file, clean these event
	tm.keyEvent = make(map[string]*Event)

	return nil
}

func (tm *TableMerge) writeBinlog(binlog *pb.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = tm.binlogger.WriteTail(&tb.Entity{Payload: data})
	return errors.Trace(err)
}

// read reads binlog from pb file
func (tm *TableMerge) read(file string) (chan *pb.Binlog, chan error) {
	binlogChan := make(chan *pb.Binlog, 10)
	errChan := make(chan error)

	go func() {
		f, err := os.OpenFile(file, os.O_RDONLY, 0600)
		if err != nil {
			errChan <- errors.Annotatef(err, "open file %s error", file)
			return
		}

		reader := bufio.NewReader(f)
		for {
			binlog, _, err := Decode(reader)
			if err != nil {
				if errors.Cause(err) == io.EOF {
					log.Info("read file end", zap.String("file", file))
					close(binlogChan)
					return
				} else {
					errChan <- errors.Trace(err)
					return
				}
			}

			binlogChan <- binlog
		}
	}()

	return binlogChan, errChan
}

func (tm *TableMerge) analyzeBinlog(binlog *pb.Binlog) error {
	switch binlog.Tp {
	case pb.BinlogType_DML:
		_, err := tm.handleDML(binlog)
		if err != nil {
			return err
		}
	case pb.BinlogType_DDL:
		err := ddlHandle.ExecuteDDL(string(binlog.GetDdlQuery()))
		if err != nil {
			return err
		}
		// merge DML events to several binlog and write to file, then write this DDL's binlog
		tm.FlushDMLBinlog(binlog.CommitTs - 1)
		tm.writeBinlog(binlog)

	default:
		panic("unreachable")
	}
	return nil
}

// handleDML split DML binlog to multiple Event and handle them
func (tm *TableMerge) handleDML(binlog *pb.Binlog) ([]*Event, error) {
	dml := binlog.DmlData
	if dml == nil {
		return nil, errors.New("dml binlog's data can't be empty")
	}

	for _, event := range dml.Events {
		schema := event.GetSchemaName()
		table := event.GetTableName()

		e := &event
		tp := e.GetTp()
		row := e.GetRow()

		var r *Event

		tableInfo, err := ddlHandle.GetTableInfo(schema, table)
		if err != nil {
			return nil, err
		}

		switch tp {
		case pb.EventType_Insert, pb.EventType_Delete:
			key, cols, err := getInsertAndDeleteRowKey(row, tableInfo)
			if err != nil {
				return nil, err
			}

			r = &Event{
				schema:    schema,
				table:     table,
				eventType: tp,
				oldKey:    key,
				cols:      cols,
			}

		case pb.EventType_Update:
			key, cKey, cols, err := getUpdateRowKey(row, tableInfo)
			if err != nil {
				return nil, err
			}

			r = &Event{
				schema:    schema,
				table:     table,
				eventType: tp,
				oldKey:    key,
				newKey:    cKey,
				cols:      cols,
			}

		default:
			panic("unreachable")
		}

		tm.HandleEvent(r)
	}

	return nil, nil
}

// HandleEvent handles event, if event's key already exist, then merge this event
// otherwise save this event
func (tm *TableMerge) HandleEvent(row *Event) {
	key := row.oldKey
	tp := row.eventType
	oldRow, ok := tm.keyEvent[key]
	if ok {
		oldRow.Merge(row)
		if oldRow.isDeleted {
			delete(tm.keyEvent, key)
			return
		}

		if tp == pb.EventType_Update {
			// update may change pk/uk value, so key may be changed
			delete(tm.keyEvent, key)
			tm.keyEvent[oldRow.oldKey] = oldRow
		}
	} else {
		tm.keyEvent[row.oldKey] = row
	}
}

// parserSchemaTableFromDDL parses ddl query to get schema and table
// ddl like `use test; create table`
func rewriteDDL(binlog *pb.Binlog) (*pb.Binlog, error) {
	var ddl []byte
	stmts, _, err := parser.New().Parse(string(binlog.DdlQuery), "", "")

	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.CreateDatabaseStmt:
			continue
		case *ast.DropDatabaseStmt:
			tbs, err := ddlHandle.getAllTableNames(node.Name)
			if err != nil {
				return nil, err
			}
			for _, v := range tbs {
				sql := fmt.Sprintf("DROP TABLE %s;", v)
				ddl = append(ddl, sql...)
			}
		default:
			var sb strings.Builder
			err = node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			if err != nil {
				return nil, err
			}
			ddl = append(ddl, sb.String()...)
			ddl = append(ddl, ';')
		}
	}
	if ddl == nil {
		return nil, nil
	}
	binlog.DdlQuery = ddl
	return binlog, nil
}

func newDMLBinlog(commitTS int64) *pb.Binlog {
	return &pb.Binlog{
		Tp:       pb.BinlogType_DML,
		CommitTs: commitTS,
		DmlData: &pb.DMLData{
			Events: make([]pb.Event, 0, 1000),
		},
	}
}

func rewriteDML(ev *pb.Event) ([]*pb.Event, error) {
	var res []*pb.Event
	switch ev.GetTp() {
	case pb.EventType_Insert, pb.EventType_Delete:
		res = append(res, ev)
	case pb.EventType_Update:
		c, err := getImageRow(ev.GetRow(), beforeImageRow)
		if err != nil {
			return nil, err
		}
		del := &pb.Event{
			SchemaName: ev.SchemaName,
			TableName:  ev.TableName,
			Tp:         pb.EventType_Delete,
			Row:        c,
		}
		res = append(res, del)

		c, err = getImageRow(ev.GetRow(), afterImageRow)
		if err != nil {
			return nil, err
		}
		ins := &pb.Event{
			SchemaName: ev.SchemaName,
			TableName:  ev.TableName,
			Tp:         pb.EventType_Insert,
			Row:        c,
		}
		res = append(res, ins)
	default:
		panic("unreachable")
	}
	return res, nil
}

func getImageRow(row [][]byte, t byte) ([][]byte, error) {
	var allColBytes [][]byte
	var bt []byte
	var err error
	for _, c := range row {
		col := &pb.Column{}
		err = col.Unmarshal(c)
		if err != nil {
			return nil, nil
		}
		var column *pb.Column
		if t == beforeImageRow {
			column = &pb.Column{
				Name:      col.Name,
				Tp:        col.Tp,
				MysqlType: col.MysqlType,
				Value:     col.Value,
			}
		} else {
			column = &pb.Column{
				Name:      col.Name,
				Tp:        col.Tp,
				MysqlType: col.MysqlType,
				Value:     col.ChangedValue,
			}
		}
		bt, err = column.Marshal()
		if err != nil {
			return nil, err
		}
		allColBytes = append(allColBytes, bt)
	}
	return allColBytes, nil
}
