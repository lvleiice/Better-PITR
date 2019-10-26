package pitr

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/format"
	"gotest.tools/assert"
)

func TestGetAllDatabaseNames(t *testing.T) {
	sql := "use test; create table t1 (a int)"
	sql1 := "create database test1"

	os.RemoveAll(defaultTiDBDir)
	ddl, err := NewDDLHandle()
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL(sql)
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL(sql1)
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

	err = ddl.ExecuteDDL(sql)
	fmt.Printf("## %v\n", err)
	assert.Assert(t, err == nil)

	var ns []string
	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 2)

	err = ddl.ExecuteDDL(sql)
	assert.Assert(t, err != nil)

	err = ddl.ResetDB()
	assert.Assert(t, err == nil)

	ns, err = ddl.getAllDatabaseNames()
	assert.Assert(t, err == nil)
	assert.Assert(t, len(ns) == 1)

	err = ddl.ExecuteDDL(sql)
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
	err = ddl.ExecuteDDL(sql)
	assert.Assert(t, err == nil)

	err = ddl.ExecuteDDL(sql1)
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
