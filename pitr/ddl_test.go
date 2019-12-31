package pitr

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"gotest.tools/assert"
)

func TestGetAllDatabaseNames(t *testing.T) {
	sql := "use test; create table t1 (a int)"
	sql1 := "create database test1"

	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL("", sql)
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL("", sql1)
	assert.Assert(t, err == nil)

	var n []string
	n, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)

	assert.Assert(t, len(n) == 2)

	for _, v := range n {
		fmt.Printf("## %s\n", v)
	}
}

func TestResetDB(t *testing.T) {
	sql := "create database test1"
	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	assert.Assert(t, err == nil)

	err = ddl.ResetDB()
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL("", sql)
	fmt.Printf("## %v\n", err)
	assert.Assert(t, err == nil)

	var ns []string
	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 2)

	err = ddl.ResetDB()
	assert.Assert(t, err == nil)

	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 1)

	err = ddl.ExecuteDDL("", sql)
	assert.Assert(t, err == nil)

	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 2)
}

func TestRestore(t *testing.T) {
	stmts, _, err := parser.New().Parse("create database test1", "", "")
	assert.Assert(t, err == nil)
	var sb strings.Builder
	for _, stmt := range stmts {
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		assert.Assert(t, err == nil)
		fmt.Printf("## %s\n", sb.String())
	}
}

func TestGetAllTableNames(t *testing.T) {
	sql := "create database test1"
	sql1 := "use test1; create table t1(a int)"
	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	ddl.ResetDB()
	assert.Assert(t, err == nil)
	err = ddl.ExecuteDDL("", sql)
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL("", sql1)
	assert.Assert(t, err == nil)

	var s []string
	s, err = ddl.getAllTableNames("test1")
	assert.Assert(t, err == nil)
	assert.Assert(t, len(s) == 1)
}

func TestFetchMapKeyFromDB(t *testing.T) {
	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	assert.Assert(t, err == nil)
	ddl.ResetDB()

	err = ddl.createMapTable()
	assert.Assert(t, err == nil)

	var key string
	key, err = ddl.fetchMapKeyFromDB("mt")
	assert.Assert(t, err == nil)
	assert.Assert(t, key == "")

	err = ddl.insertMapKeyFromDB("mt", "mt_src")
	assert.Assert(t, err == nil)
	key, err = ddl.fetchMapKeyFromDB("mt")
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("mt_src", key))

	err = ddl.insertMapKeyFromDB("mt_dst", "mt")
	assert.Assert(t, err == nil)
	key, err = ddl.fetchMapKeyFromDB("mt_dst")
	assert.Assert(t, err == nil)
	assert.Assert(t, strings.EqualFold("mt_src", key))
}

func TestSkipJob(t *testing.T) {
	testCases := []struct {
		job       *model.Job
		isSkipJob bool
	}{
		{
			&model.Job{
				State: model.JobStateSynced,
			},
			false,
		}, {
			&model.Job{
				State: model.JobStateDone,
			},
			false,
		}, {
			&model.Job{
				State: model.JobStateCancelling,
			},
			true,
		},
	}

	for _, tc := range testCases {
		isSkipJob := skipJob(tc.job)
		assert.Assert(t, isSkipJob == tc.isSkipJob)
	}

}

func TestExecuteDDLs(t *testing.T) {
	os.RemoveAll(defaultTiDBDir)
	ddlHandle, err := NewDDLHandle()
	fmt.Println(err)
	assert.Assert(t, err == nil)

	historyDDLs := []*model.Job{
		{
			Query: "create database unit_test",
			State: model.JobStateDone,
		}, {
			Query: "create table unit_test.t1(id int)",
			State: model.JobStateDone,
		}, {
			Query: "create table t2(id int)",
			State: model.JobStateDone,
			BinlogInfo: &model.HistoryInfo{
				DBInfo: &model.DBInfo{
					Name: model.NewCIStr("unit_test"),
				},
			},
		}, {
			Query: "create table unit_test.t3(id int)",
			State: model.JobStateCancelling,
		},
	}

	err = ddlHandle.ExecuteHistoryDDLs(historyDDLs)
	assert.Assert(t, err == nil)

	err = ddlHandle.ExecuteDDL("", "create table ")
	assert.Assert(t, err != nil)

	err = ddlHandle.ExecuteDDL("", "create database unit_test")
	assert.Assert(t, err == nil)

	tableInfo, err := ddlHandle.GetTableInfo("unit_test", "t1")
	assert.Assert(t, err == nil)
	assert.Assert(t, tableInfo.columns[0] == "id")

	tableInfo, err = ddlHandle.GetTableInfo("unit_test", "t5")
	assert.Assert(t, err != nil)
}

func TestParserSchemaTableFromDDL(t *testing.T) {
	testCases := []struct {
		ddl    string
		schema string
		table  string
	}{
		{
			"truncate table test.t1",
			"test",
			"t1",
		}, {
			"alter table test.t1 add index a(id)",
			"test",
			"t1",
		}, {
			"alter table test.t1 drop index a",
			"test",
			"t1",
		}, {
			"rename table test.t1 to test.t2",
			"test",
			"t2",
		}, {
			"create index a on test.t1(id)",
			"test",
			"t1",
		}, {
			"drop index a on test.t1",
			"test",
			"t1",
		},
	}

	for _, tc := range testCases {
		schema, table, err := parserSchemaTableFromDDL(tc.ddl)
		assert.Assert(t, err == nil)
		assert.Assert(t, tc.schema == schema)
		assert.Assert(t, tc.table == table)
	}

	invalidDDL := "use test; alter table test.t1 add index a(id), drop index a on test.t1"
	_, _, err := parserSchemaTableFromDDL(invalidDDL)
	assert.Assert(t, err != nil)
}
