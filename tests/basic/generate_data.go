package main

import (
	"flag"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/tests/dailytest"
	"github.com/pingcap/tidb-binlog/tests/util"
)

func main() {
	cfg := util.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()

	tableSQLs := []string{`
	create table ptest(
		a int primary key,
		b double NOT NULL DEFAULT 2.0,
		c varchar(10) NOT NULL,
		d time unique
	);
	`,
		`
	create table itest(
		a int,
		b double NOT NULL DEFAULT 2.0,
		c varchar(10) NOT NULL,
		d time unique,
		PRIMARY KEY(a, b)
	);
	`,
		`
	create table ntest(
		a int,
		b double NOT NULL DEFAULT 2.0,
		c varchar(10) NOT NULL,
		d time unique
	);
	`}

	dailytest.RunDailyTest(sourceDB, tableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch)
}
