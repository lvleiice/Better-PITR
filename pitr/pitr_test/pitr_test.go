package pitr_test

import (
	"database/sql"
	"sort"
	"testing"
	"time"

	. "github.com/WangXiangUSTC/tidb-lite"
	"github.com/lvleiice/Better-PITR/pitr"
	. "github.com/pingcap/check"
)

func TestPITR(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testPITRSuite{})

type testPITRSuite struct{}

func (t *testPITRSuite) TestPITRDDL(c *C) {
	tidbServer1, err := NewTiDBServer(NewOptions(c.MkDir()).WithPort(4040))
	c.Assert(err, IsNil)
	defer tidbServer1.Close()

	var dbConn *sql.DB
	for i := 0; i < 5; i++ {
		dbConn, err = tidbServer1.CreateConn()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	c.Assert(err, IsNil)

	// add huge ddl history job by executing ddl string.
	ddls := getDDLs()
	for _, ddl := range ddls {
		if ddl.ok {
			result, err := dbConn.Query(ddl.src)
			c.Assert(err, IsNil)
			result.Close()
		}
	}
	// get ddl history job slice.
	snapMeta, err := pitr.GetSnapshotMeta(tidbServer1.GetStorage())
	c.Assert(err, IsNil)
	allJobs, err := snapMeta.GetAllHistoryDDLJobs()
	c.Assert(err, IsNil)

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(allJobs, func(i, j int) bool {
		return allJobs[i].BinlogInfo.SchemaVersion < allJobs[j].BinlogInfo.SchemaVersion
	})

	// show origin tables
	result, err := dbConn.Query("show databases")
	c.Assert(err, IsNil)
	dbMap := make(map[string]map[string]string,0)
	var dbString sql.NullString
	for result.Next() {
		err := result.Scan(&dbString)
		c.Assert(err, IsNil)
		if dbString.Valid && !shouldIgnoreDB(dbString.String){
			dbMap[dbString.String] = make(map[string]string,0)
		}
	}
	result.Close()
	for dk, dv := range dbMap {
		result, err = dbConn.Query("show tables from " + dk)
		c.Assert(err, IsNil)
		var tableString sql.NullString
		for result.Next() {
			err := result.Scan(&tableString)
			c.Assert(err, IsNil)
			if tableString.Valid {
				dv[tableString.String] = ""
			}
		}
		result.Close()
		for tk, _ := range dv {
			result, err = dbConn.Query("show create table " + dk+"."+tk)
			c.Assert(err, IsNil)
			var nameString sql.NullString
			var createTableString sql.NullString
			for result.Next() {
				err := result.Scan(&nameString, &createTableString)
				c.Assert(err, IsNil)
				if createTableString.Valid {
					dv[tk] = createTableString.String
					break
				}
			}
			result.Close()
		}
	}
	// fmt.Println(dbMap)

	// scan the ddl job list summarise the latest DBInfo and TableInfo.
	dHanle := &pitr.DDLHandle{}
	dHanle.SetServerHistoryAccelerate(tidbServer1, allJobs, true)
	for _, job := range allJobs {
		err = dHanle.AccelerateHistoryDDLs(job)
		c.Assert(err, IsNil)
	}
	err = dHanle.ShiftMetaToTiDB()
	c.Assert(err, IsNil)
	err = tidbServer1.GetDomain().Reload()
	c.Assert(err, IsNil)

	// check the meta(show create table) equal to origin output.
	result, err = dbConn.Query("show databases")
	c.Assert(err, IsNil)
	var dbs []string
	for result.Next() {
		err := result.Scan(&dbString)
		c.Assert(err, IsNil)
		if dbString.Valid && !shouldIgnoreDB(dbString.String){
			_, ok := dbMap[dbString.String]
			c.Assert(ok, Equals, true)
			dbs = append(dbs, dbString.String)
		}
	}
	result.Close()
	c.Assert(len(dbMap), Equals, len(dbs))
	for _, db := range dbs {
		var tables []string
		result, err = dbConn.Query("show tables from " + db)
		c.Assert(err, IsNil)
		var tableString sql.NullString
		for result.Next() {
			err := result.Scan(&tableString)
			c.Assert(err, IsNil)
			if tableString.Valid {
				_, ok := dbMap[db][tableString.String]
				c.Assert(ok, Equals, true)
				tables = append(tables, tableString.String)
			}
		}
		result.Close()
		for _, table := range tables {
			result, err = dbConn.Query("show create table " + db+"."+table)
			c.Assert(err, IsNil)
			var nameString sql.NullString
			var createTableString sql.NullString
			for result.Next() {
				err := result.Scan(&nameString, &createTableString)
				c.Assert(err, IsNil)
				if createTableString.Valid {
					originCreateTable, ok := dbMap[db][table]
					c.Assert(ok, Equals, true)
					c.Assert(createTableString.String, Equals, originCreateTable)
				}
				break
			}
			result.Close()
		}
	}
}

func shouldIgnoreDB(dbName string) bool {
	if dbName == "INFORMATION_SCHEMA" || dbName == "PERFORMANCE_SCHEMA" || dbName == "mysql" {
		return true
	}
	return false
}

type testCase struct {
	src     string
	ok      bool
	restore string
}

func getDDLs() []testCase {
	ddls := []testCase{
		/*
		 * port from tidb parser_test.go, ignore the restore string.
		 */
		{"CREATE", false, ""},
		{"CREATE TABLE", false, ""},
		{"CREATE TABLE test.foo (", false, ""},
		{"CREATE TABLE test.foo ()", false, ""},
		{"CREATE TABLE test.foo ();", false, ""},
		{"CREATE TABLE test.foo1 (a varchar(50), b int);", true, "CREATE TABLE `foo` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE test.foo2 (a TINYINT UNSIGNED);", true, "CREATE TABLE `foo` (`a` TINYINT UNSIGNED)"},
		{"CREATE TABLE test.foo3 (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE test.foo4 (a bigint unsigned, b bool);", true, "CREATE TABLE `foo` (`a` BIGINT UNSIGNED,`b` TINYINT(1))"},
		{"CREATE TABLE test.foo5 (a TINYINT, b SMALLINT) CREATE TABLE bar (x INT, y int64)", false, ""},
		{"CREATE TABLE test.foo6 (a int, b float); CREATE TABLE test.bar (x double, y float)", true, "CREATE TABLE `foo` (`a` INT,`b` FLOAT); CREATE TABLE `bar` (`x` DOUBLE,`y` FLOAT)"},
		{"CREATE TABLE test.foo7 (a bytes)", false, ""},
		{"CREATE TABLE test.foo8 (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE test.foo9 (a SMALLINT UNSIGNED, b INT UNSIGNED) -- foo", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE test.foo10 (a SMALLINT UNSIGNED, b INT UNSIGNED) // foo", false, ""},
		{"CREATE TABLE test.foo11 (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE test.foo12 /* foo */ (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE test.foo13 (name CHAR(50) BINARY);", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY)"},
		{"CREATE TABLE test.foo14 (name CHAR(50) COLLATE utf8_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE utf8_bin)"},
		{"CREATE TABLE test.foo15 (id varchar(50) collate utf8_bin);", true, "CREATE TABLE `foo` (`id` VARCHAR(50) COLLATE utf8_bin)"},
		{"CREATE TABLE test.foo16 (name CHAR(50) CHARACTER SET UTF8)", true, "CREATE TABLE `foo` (`name` CHAR(50) CHARACTER SET UTF8)"},
		{"CREATE TABLE test.foo17 (name CHAR(50) CHARACTER SET utf8 BINARY)", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY CHARACTER SET UTF8)"},
		{"CREATE TABLE test.foo18 (name CHAR(50) CHARACTER SET utf8 BINARY CHARACTER set utf8)", false, ""},
		{"CREATE TABLE test.foo19 (name CHAR(50) BINARY CHARACTER SET utf8 COLLATE utf8_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY CHARACTER SET UTF8 COLLATE utf8_bin)"},
		{"CREATE TABLE test.foo20 (name CHAR(50) CHARACTER SET utf8 COLLATE utf8_bin COLLATE ascii_bin)", false, "CREATE TABLE `foo` (`name` CHAR(50) CHARACTER SET UTF8 COLLATE utf8_bin COLLATE ascii_bin)"},
		{"CREATE TABLE test.foo21 (name CHAR(50) COLLATE ascii_bin COLLATE latin1_bin)", false, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE ascii_bin COLLATE latin1_bin)"},
		{"CREATE TABLE test.foo22 (name CHAR(50) COLLATE ascii_bin PRIMARY KEY COLLATE latin1_bin)", false, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE ascii_bin PRIMARY KEY COLLATE latin1_bin)"},
		{"CREATE TABLE test.foo23 (a.b, b);", false, ""},
		{"CREATE TABLE test.foo24 (a, b.c);", false, ""},
		{"CREATE TABLE (name CHAR(50) BINARY)", false, ""},
		// for create temporary table
		{"CREATE TABLE test.t (a varchar(50), b int);", true, "CREATE TEMPORARY TABLE `t` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TEMPORARY TABLE test.t1 (a varchar(50), b int);", true, "CREATE TEMPORARY TABLE `t` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TEMPORARY TABLE test.t2 LIKE test.t1", true, "CREATE TEMPORARY TABLE `t` LIKE `t1`"},
		{"DROP TEMPORARY TABLE IF EXISTS test.t3", true, "DROP TEMPORARY TABLE `t`"},
		// test use key word as column name
		{"CREATE TABLE test.foo25 (pump varchar(50), b int);", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE test.foo26 (drainer varchar(50), b int);", true, "CREATE TABLE `foo` (`drainer` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE test.foo27 (node_id varchar(50), b int);", true, "CREATE TABLE `foo` (`node_id` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE test.foo28 (node_state varchar(50), b int);", true, "CREATE TABLE `foo` (`node_state` VARCHAR(50),`b` INT)"},
		// for table option
		{"create table test.t4 (c int) avg_row_length = 3", true, "CREATE TABLE `t` (`c` INT) AVG_ROW_LENGTH = 3"},
		{"create table test.t5 (c int) avg_row_length 3", true, "CREATE TABLE `t` (`c` INT) AVG_ROW_LENGTH = 3"},
		{"create table test.t6 (c int) checksum = 0", true, "CREATE TABLE `t` (`c` INT) CHECKSUM = 0"},
		{"create table test.t7 (c int) checksum 1", true, "CREATE TABLE `t` (`c` INT) CHECKSUM = 1"},
		{"create table test.t8 (c int) table_checksum = 0", true, "CREATE TABLE `t` (`c` INT) TABLE_CHECKSUM = 0"},
		{"create table test.t9 (c int) table_checksum 1", true, "CREATE TABLE `t` (`c` INT) TABLE_CHECKSUM = 1"},
		{"create table test.t10 (c int) compression = 'NONE'", true, "CREATE TABLE `t` (`c` INT) COMPRESSION = 'NONE'"},
		{"create table test.t11 (c int) compression 'lz4'", true, "CREATE TABLE `t` (`c` INT) COMPRESSION = 'lz4'"},
		{"create table test.t12 (c int) connection = 'abc'", true, "CREATE TABLE `t` (`c` INT) CONNECTION = 'abc'"},
		{"create table test.t13 (c int) connection 'abc'", true, "CREATE TABLE `t` (`c` INT) CONNECTION = 'abc'"},
		{"create table test.t14 (c int) key_block_size = 1024", true, "CREATE TABLE `t` (`c` INT) KEY_BLOCK_SIZE = 1024"},
		{"create table test.t15 (c int) key_block_size 1024", true, "CREATE TABLE `t` (`c` INT) KEY_BLOCK_SIZE = 1024"},
		{"create table test.t16 (c int) max_rows = 1000", true, "CREATE TABLE `t` (`c` INT) MAX_ROWS = 1000"},
		{"create table test.t17 (c int) max_rows 1000", true, "CREATE TABLE `t` (`c` INT) MAX_ROWS = 1000"},
		{"create table test.t18 (c int) min_rows = 1000", true, "CREATE TABLE `t` (`c` INT) MIN_ROWS = 1000"},
		{"create table test.t19 (c int) min_rows 1000", true, "CREATE TABLE `t` (`c` INT) MIN_ROWS = 1000"},
		{"create table test.t20 (c int) password = 'abc'", true, "CREATE TABLE `t` (`c` INT) PASSWORD = 'abc'"},
		{"create table test.t21 (c int) password 'abc'", true, "CREATE TABLE `t` (`c` INT) PASSWORD = 'abc'"},
		{"create table test.t22 (c int) DELAY_KEY_WRITE=1", true, "CREATE TABLE `t` (`c` INT) DELAY_KEY_WRITE = 1"},
		{"create table test.t23 (c int) DELAY_KEY_WRITE 1", true, "CREATE TABLE `t` (`c` INT) DELAY_KEY_WRITE = 1"},
		{"create table test.t24 (c int) ROW_FORMAT = default", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DEFAULT"},
		{"create table test.t25 (c int) ROW_FORMAT default", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DEFAULT"},
		{"create table test.t26 (c int) ROW_FORMAT = fixed", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = FIXED"},
		{"create table test.t27 (c int) ROW_FORMAT = compressed", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = COMPRESSED"},
		{"create table test.t28 (c int) ROW_FORMAT = compact", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = COMPACT"},
		{"create table test.t29 (c int) ROW_FORMAT = redundant", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = REDUNDANT"},
		{"create table test.t30 (c int) ROW_FORMAT = dynamic", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DYNAMIC"},
		{"create table test.t31 (c int) STATS_PERSISTENT = default", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table test.t32 (c int) STATS_PERSISTENT = 0", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table test.t33 (c int) STATS_PERSISTENT = 1", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table test.t34 (c int) STATS_SAMPLE_PAGES 0", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = 0"},
		{"create table test.t35 (c int) STATS_SAMPLE_PAGES 10", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = 10"},
		{"create table test.t36 (c int) STATS_SAMPLE_PAGES = 10", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = 10"},
		{"create table test.t37 (c int) STATS_SAMPLE_PAGES = default", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = DEFAULT"},
		{"create table test.t38 (c int) PACK_KEYS = 1", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table test.t39 (c int) PACK_KEYS = 0", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table test.t40 (c int) PACK_KEYS = DEFAULT", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table test.t41 (c int) STORAGE DISK", true, "CREATE TABLE `t` (`c` INT) STORAGE DISK"},
		{"create table test.t42 (c int) STORAGE MEMORY", true, "CREATE TABLE `t` (`c` INT) STORAGE MEMORY"},
		{"create table test.t43 (c int) SECONDARY_ENGINE null", true, "CREATE TABLE `t` (`c` INT) SECONDARY_ENGINE = NULL"},
		{"create table test.t44 (c int) SECONDARY_ENGINE = innodb", true, "CREATE TABLE `t` (`c` INT) SECONDARY_ENGINE = 'innodb'"},
		{"create table test.t45 (c int) SECONDARY_ENGINE 'null'", true, "CREATE TABLE `t` (`c` INT) SECONDARY_ENGINE = 'null'"},
		{`create table test.testTableCompression (c VARCHAR(15000)) compression="ZLIB";`, true, "CREATE TABLE `testTableCompression` (`c` VARCHAR(15000)) COMPRESSION = 'ZLIB'"},
		{`create table test.t46 (c1 int) compression="zlib";`, true, "CREATE TABLE `t1` (`c1` INT) COMPRESSION = 'zlib'"},

		// partition option
		{"CREATE TABLE test.t47 (id int) ENGINE = INNDB PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20));", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10),PARTITION `p1` VALUES LESS THAN (20))"},
		{"create table test.t48 (c int) PARTITION BY HASH (c) PARTITIONS 32;", true, "CREATE TABLE `t` (`c` INT) PARTITION BY HASH (`c`) PARTITIONS 32"},
		{"create table test.t49 (c int) PARTITION BY HASH (Year(VDate)) (PARTITION p1980 VALUES LESS THAN (1980) ENGINE = MyISAM, PARTITION p1990 VALUES LESS THAN (1990) ENGINE = MyISAM, PARTITION pothers VALUES LESS THAN MAXVALUE ENGINE = MyISAM)", false, ""},
		{"create table test.t50 (c int) PARTITION BY RANGE (Year(VDate)) (PARTITION p1980 VALUES LESS THAN (1980) ENGINE = MyISAM, PARTITION p1990 VALUES LESS THAN (1990) ENGINE = MyISAM, PARTITION pothers VALUES LESS THAN MAXVALUE ENGINE = MyISAM)", false, "CREATE TABLE `t` (`c` INT) PARTITION BY RANGE (YEAR(`VDate`)) (PARTITION `p1980` VALUES LESS THAN (1980) ENGINE = MyISAM,PARTITION `p1990` VALUES LESS THAN (1990) ENGINE = MyISAM,PARTITION `pothers` VALUES LESS THAN (MAXVALUE) ENGINE = MyISAM)"},
		{"create table test.t51 (c int, `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '') PARTITION BY RANGE (UNIX_TIMESTAMP(create_time)) (PARTITION p201610 VALUES LESS THAN(1477929600), PARTITION p201611 VALUES LESS THAN(1480521600),PARTITION p201612 VALUES LESS THAN(1483200000),PARTITION p201701 VALUES LESS THAN(1485878400),PARTITION p201702 VALUES LESS THAN(1488297600),PARTITION p201703 VALUES LESS THAN(1490976000))", true, "CREATE TABLE `t` (`c` INT,`create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '') PARTITION BY RANGE (UNIX_TIMESTAMP(`create_time`)) (PARTITION `p201610` VALUES LESS THAN (1477929600),PARTITION `p201611` VALUES LESS THAN (1480521600),PARTITION `p201612` VALUES LESS THAN (1483200000),PARTITION `p201701` VALUES LESS THAN (1485878400),PARTITION `p201702` VALUES LESS THAN (1488297600),PARTITION `p201703` VALUES LESS THAN (1490976000))"},
		{"CREATE TABLE test.t52 (`shopCode` varchar(4) DEFAULT NULL COMMENT '地点') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!50100 PARTITION BY KEY (shopCode) PARTITIONS 19 */;", true, "CREATE TABLE `md_product_shop` (`shopCode` VARCHAR(4) DEFAULT NULL COMMENT '地点') ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 PARTITION BY KEY (`shopCode`) PARTITIONS 19"},
		{"CREATE TABLE test.t53 (`id` bigint(20) NOT NULL AUTO_INCREMENT, `oderTime` datetime NOT NULL, primary key(id)) ENGINE=InnoDB AUTO_INCREMENT=641533032 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 /*!50500 PARTITION BY RANGE COLUMNS(oderTime) (PARTITION P2011 VALUES LESS THAN ('2012-01-01 00:00:00') ENGINE = InnoDB, PARTITION P1201 VALUES LESS THAN ('2012-02-01 00:00:00') ENGINE = InnoDB, PARTITION PMAX VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)*/;", false, "CREATE TABLE `payinfo1` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`oderTime` DATETIME NOT NULL) ENGINE = InnoDB AUTO_INCREMENT = 641533032 DEFAULT CHARACTER SET = UTF8 ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8 PARTITION BY RANGE COLUMNS (`oderTime`) (PARTITION `P2011` VALUES LESS THAN ('2012-01-01 00:00:00') ENGINE = InnoDB,PARTITION `P1201` VALUES LESS THAN ('2012-02-01 00:00:00') ENGINE = InnoDB,PARTITION `PMAX` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)"},
		{`CREATE TABLE test.t54 (id bigint(20) NOT NULL, app_version varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'default', gmt_create datetime NOT NULL COMMENT '创建时间', PRIMARY KEY (gmt_create)) ENGINE=InnoDB AUTO_INCREMENT=33703438 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
/*!50100 PARTITION BY RANGE (month(gmt_create)-1)
(PARTITION part0 VALUES LESS THAN (1) COMMENT = '1月份' ENGINE = InnoDB,
 PARTITION part1 VALUES LESS THAN (2) COMMENT = '2月份' ENGINE = InnoDB,
 PARTITION part2 VALUES LESS THAN (3) COMMENT = '3月份' ENGINE = InnoDB,
 PARTITION part3 VALUES LESS THAN (4) COMMENT = '4月份' ENGINE = InnoDB,
 PARTITION part4 VALUES LESS THAN (5) COMMENT = '5月份' ENGINE = InnoDB,
 PARTITION part5 VALUES LESS THAN (6) COMMENT = '6月份' ENGINE = InnoDB,
 PARTITION part6 VALUES LESS THAN (7) COMMENT = '7月份' ENGINE = InnoDB,
 PARTITION part7 VALUES LESS THAN (8) COMMENT = '8月份' ENGINE = InnoDB,
 PARTITION part8 VALUES LESS THAN (9) COMMENT = '9月份' ENGINE = InnoDB,
 PARTITION part9 VALUES LESS THAN (10) COMMENT = '10月份' ENGINE = InnoDB,
 PARTITION part10 VALUES LESS THAN (11) COMMENT = '11月份' ENGINE = InnoDB,
 PARTITION part11 VALUES LESS THAN (12) COMMENT = '12月份' ENGINE = InnoDB) */ ;`, true, "CREATE TABLE `app_channel_daily_report` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`app_version` VARCHAR(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'default',`gmt_create` DATETIME NOT NULL COMMENT '创建时间',PRIMARY KEY(`id`)) ENGINE = InnoDB AUTO_INCREMENT = 33703438 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_UNICODE_CI PARTITION BY RANGE (MONTH(`gmt_create`)-1) (PARTITION `part0` VALUES LESS THAN (1) COMMENT = '1月份' ENGINE = InnoDB,PARTITION `part1` VALUES LESS THAN (2) COMMENT = '2月份' ENGINE = InnoDB,PARTITION `part2` VALUES LESS THAN (3) COMMENT = '3月份' ENGINE = InnoDB,PARTITION `part3` VALUES LESS THAN (4) COMMENT = '4月份' ENGINE = InnoDB,PARTITION `part4` VALUES LESS THAN (5) COMMENT = '5月份' ENGINE = InnoDB,PARTITION `part5` VALUES LESS THAN (6) COMMENT = '6月份' ENGINE = InnoDB,PARTITION `part6` VALUES LESS THAN (7) COMMENT = '7月份' ENGINE = InnoDB,PARTITION `part7` VALUES LESS THAN (8) COMMENT = '8月份' ENGINE = InnoDB,PARTITION `part8` VALUES LESS THAN (9) COMMENT = '9月份' ENGINE = InnoDB,PARTITION `part9` VALUES LESS THAN (10) COMMENT = '10月份' ENGINE = InnoDB,PARTITION `part10` VALUES LESS THAN (11) COMMENT = '11月份' ENGINE = InnoDB,PARTITION `part11` VALUES LESS THAN (12) COMMENT = '12月份' ENGINE = InnoDB)"},

		// for check clause
		{"create table test.t55 (c1 bool, c2 bool, check (c1 in (0, 1)) not enforced, check (c2 in (0, 1)))", true, "CREATE TABLE `t` (`c1` TINYINT(1),`c2` TINYINT(1),CHECK(`c1` IN (0,1)) NOT ENFORCED,CHECK(`c2` IN (0,1)) ENFORCED)"},
		{"CREATE TABLE test.Customer (SD integer CHECK (SD > 0), First_Name varchar(30));", true, "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) ENFORCED,`First_Name` VARCHAR(30))"},
		{"CREATE TABLE test.Customer1 (SD integer CHECK (SD > 0) not enforced, SS varchar(30) check(ss='test') enforced);", true, "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) NOT ENFORCED,`SS` VARCHAR(30) CHECK(`ss`='test') ENFORCED)"},
		{"CREATE TABLE test.Customer2 (SD integer CHECK (SD > 0) not null, First_Name varchar(30) comment 'string' not null);", true, "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) ENFORCED NOT NULL,`First_Name` VARCHAR(30) COMMENT 'string' NOT NULL)"},
		{"CREATE TABLE test.Customer3 (SD integer comment 'string' CHECK (SD > 0) not null);", true, "CREATE TABLE `Customer` (`SD` INT COMMENT 'string' CHECK(`SD`>0) ENFORCED NOT NULL)"},
		{"CREATE TABLE test.Customer4 (SD integer comment 'string' not enforced, First_Name varchar(30));", false, ""},
		{"CREATE TABLE test.Customer5 (SD integer not enforced, First_Name varchar(30));", false, ""},

		{"create database xxx", true, "CREATE DATABASE `xxx`"},
		{"create database if exists xxx", false, ""},
		{"create database if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},

		// for create database with encryption
		{"create database xxx encryption = 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"create database xxx encryption 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"create database xxx default encryption = 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"create database xxx default encryption 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"create database xxx encryption = 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"create database xxx encryption 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"create database xxx1 default encryption = 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"create database xxx2 default encryption 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"create database xxx encryption = N", false, ""},
		{"create schema if exists xxx", false, ""},
		{"create schema if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},

		// for issue 974
		{`CREATE TABLE test.address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET UTF8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`),INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		// for issue 975
		{`CREATE TABLE test.test_data (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(255) NOT NULL,
		amount decimal(19,2) DEFAULT NULL,
		charge_id varchar(32) DEFAULT NULL,
		paid_amount decimal(19,2) DEFAULT NULL,
		transaction_no varchar(64) DEFAULT NULL,
		wx_mp_app_id varchar(32) DEFAULT NULL,
		contacts varchar(50) DEFAULT NULL,
		deliver_fee decimal(19,2) DEFAULT NULL,
		deliver_info varchar(255) DEFAULT NULL,
		deliver_time varchar(255) DEFAULT NULL,
		description varchar(255) DEFAULT NULL,
		invoice varchar(255) DEFAULT NULL,
		order_from int(11) DEFAULT NULL,
		order_state int(11) NOT NULL,
		packing_fee decimal(19,2) DEFAULT NULL,
		payment_time datetime DEFAULT NULL,
		payment_type int(11) DEFAULT NULL,
		phone varchar(50) NOT NULL,
		store_employee_id bigint(20) DEFAULT NULL,
		store_id bigint(20) NOT NULL,
		user_id bigint(20) NOT NULL,
		payment_mode int(11) NOT NULL,
		current_latitude double NOT NULL,
		current_longitude double NOT NULL,
		address_latitude double NOT NULL,
		address_longitude double NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT food_order_ibfk_1 FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		CONSTRAINT food_order_ibfk_2 FOREIGN KEY (store_id) REFERENCES waimaiqa.store (id),
		CONSTRAINT food_order_ibfk_3 FOREIGN KEY (store_employee_id) REFERENCES waimaiqa.store_employee (id),
		UNIQUE FK_UNIQUE_charge_id USING BTREE (charge_id) comment '',
		INDEX FK_eqst2x1xisn3o0wbrlahnnqq8 USING BTREE (store_employee_id) comment '',
		INDEX FK_8jcmec4kb03f4dod0uqwm54o9 USING BTREE (store_id) comment '',
		INDEX FK_a3t0m9apja9jmrn60uab30pqd USING BTREE (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=95 DEFAULT CHARACTER SET utf8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `test_data` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(255) NOT NULL,`amount` DECIMAL(19,2) DEFAULT NULL,`charge_id` VARCHAR(32) DEFAULT NULL,`paid_amount` DECIMAL(19,2) DEFAULT NULL,`transaction_no` VARCHAR(64) DEFAULT NULL,`wx_mp_app_id` VARCHAR(32) DEFAULT NULL,`contacts` VARCHAR(50) DEFAULT NULL,`deliver_fee` DECIMAL(19,2) DEFAULT NULL,`deliver_info` VARCHAR(255) DEFAULT NULL,`deliver_time` VARCHAR(255) DEFAULT NULL,`description` VARCHAR(255) DEFAULT NULL,`invoice` VARCHAR(255) DEFAULT NULL,`order_from` INT(11) DEFAULT NULL,`order_state` INT(11) NOT NULL,`packing_fee` DECIMAL(19,2) DEFAULT NULL,`payment_time` DATETIME DEFAULT NULL,`payment_type` INT(11) DEFAULT NULL,`phone` VARCHAR(50) NOT NULL,`store_employee_id` BIGINT(20) DEFAULT NULL,`store_id` BIGINT(20) NOT NULL,`user_id` BIGINT(20) NOT NULL,`payment_mode` INT(11) NOT NULL,`current_latitude` DOUBLE NOT NULL,`current_longitude` DOUBLE NOT NULL,`address_latitude` DOUBLE NOT NULL,`address_longitude` DOUBLE NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `food_order_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`),CONSTRAINT `food_order_ibfk_2` FOREIGN KEY (`store_id`) REFERENCES `waimaiqa`.`store`(`id`),CONSTRAINT `food_order_ibfk_3` FOREIGN KEY (`store_employee_id`) REFERENCES `waimaiqa`.`store_employee`(`id`),UNIQUE `FK_UNIQUE_charge_id`(`charge_id`) USING BTREE,INDEX `FK_eqst2x1xisn3o0wbrlahnnqq8`(`store_employee_id`) USING BTREE,INDEX `FK_8jcmec4kb03f4dod0uqwm54o9`(`store_id`) USING BTREE,INDEX `FK_a3t0m9apja9jmrn60uab30pqd`(`user_id`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 95 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		{`create table test.tttt (c int KEY);`, true, "CREATE TABLE `t` (`c` INT PRIMARY KEY)"},
		{`CREATE TABLE test.address2 (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION,INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		{"CREATE TABLE test.address3 (\r\nid bigint(20) NOT NULL AUTO_INCREMENT,\r\ncreate_at datetime NOT NULL,\r\ndeleted tinyint(1) NOT NULL,\r\nupdate_at datetime NOT NULL,\r\nversion bigint(20) DEFAULT NULL,\r\naddress varchar(128) NOT NULL,\r\naddress_detail varchar(128) NOT NULL,\r\ncellphone varchar(16) NOT NULL,\r\nlatitude double NOT NULL,\r\nlongitude double NOT NULL,\r\nname varchar(16) NOT NULL,\r\nsex tinyint(1) NOT NULL,\r\nuser_id bigint(20) NOT NULL,\r\nPRIMARY KEY (id),\r\nCONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,\r\nINDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''\r\n) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;", true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION,INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		//for issue 1802
		{`CREATE TABLE test.t56 (
		accout_id int(11) DEFAULT '0',
		summoner_id int(11) DEFAULT '0',
		union_name varbinary(52) NOT NULL,
		union_id int(11) DEFAULT '0',
		PRIMARY KEY (union_name)) ENGINE=MyISAM DEFAULT CHARSET=binary;`, true, "CREATE TABLE `t1` (`accout_id` INT(11) DEFAULT '0',`summoner_id` INT(11) DEFAULT '0',`union_name` VARBINARY(52) NOT NULL,`union_id` INT(11) DEFAULT '0',PRIMARY KEY(`union_name`)) ENGINE = MyISAM DEFAULT CHARACTER SET = BINARY"},
		// for issue pingcap/parser#310
		{`CREATE TABLE test.t57 (a DECIMAL(20,0), b DECIMAL(30), c FLOAT(25,0))`, true, "CREATE TABLE `t` (`a` DECIMAL(20,0),`b` DECIMAL(30),`c` FLOAT(25,0))"},
		// Create table with multiple index options.
		{`create table test.t58 (c int, index ci (c) USING BTREE COMMENT "123");`, true, "CREATE TABLE `t` (`c` INT,INDEX `ci`(`c`) USING BTREE COMMENT '123')"},
		// for default value
		{"CREATE TABLE test.t59 (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true, "CREATE TABLE `sbtest` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,`k` INT UNSIGNED DEFAULT '0' NOT NULL,`c` CHAR(120) DEFAULT '' NOT NULL,`pad` CHAR(60) DEFAULT '' NOT NULL,PRIMARY KEY(`id`))"},
		{"create table test.t60 (create_date TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT now());", true, "CREATE TABLE `test` (`create_date` TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT CURRENT_TIMESTAMP())"},
		{"create table test.t61 (t int, v timestamp(3) default CURRENT_TIMESTAMP(3));", true, "CREATE TABLE `ts` (`t` INT,`v` TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"}, //TODO: The number yacc in parentheses has not been implemented yet.
		// Create table with primary key name.
		{"create table if not exists test.t62 (`id` int not null auto_increment comment '消息ID', primary key `pk_id` (`id`) );", true, "CREATE TABLE IF NOT EXISTS `t` (`id` INT NOT NULL AUTO_INCREMENT COMMENT '消息ID',PRIMARY KEY `pk_id`(`id`))"},
		// Create table with like.
		{"create table test.a1(id int)", true, "CREATE TABLE `a` LIKE `b`"},
		{"create table test.a2 like test.a1", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE NO ACTION)"},
		{"create table test.a3 (id int REFERENCES test.a1 (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"},
		{"create table test.a4 (id int REFERENCES test.a1 (id) ON delete set null on update CASCADE)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE SET NULL ON UPDATE CASCADE)"},
		{"create table test.a5 (id int REFERENCES test.a1 (id) ON update set default on delete RESTRICT)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE RESTRICT ON UPDATE SET DEFAULT)"},
		{"create table test.a6 (id int REFERENCES test.a1 (id) MATCH FULL ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH FULL ON DELETE NO ACTION)"},
		{"create table test.a7 (id int REFERENCES test.a1 (id) MATCH PARTIAL ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH PARTIAL ON UPDATE NO ACTION)"},
		{"create table test.a8 (id int REFERENCES test.a1 (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"},

		{"create table test.t63 (a timestamp default now)", false, ""},
		{"create table test.t64 (a timestamp default now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"},
		{"create table test.t65 (a timestamp default now() on update now)", false, ""},
		{"create table test.t66 (a timestamp default now() on update now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP())"},
		{"CREATE TABLE test.t67 (c TEXT) default CHARACTER SET utf8, default COLLATE utf8_general_ci;", true, "CREATE TABLE `t` (`c` TEXT) DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"CREATE TABLE test.t68 (c TEXT) shard_row_id_bits = 1;", true, "CREATE TABLE `t` (`c` TEXT) SHARD_ROW_ID_BITS = 1"},
		{"CREATE TABLE test.t69 (c TEXT) shard_row_id_bits = 1, PRE_SPLIT_REGIONS = 1;", true, "CREATE TABLE `t` (`c` TEXT) SHARD_ROW_ID_BITS = 1 PRE_SPLIT_REGIONS = 1"},
		// Create table with ON UPDATE CURRENT_TIMESTAMP(6), specify fraction part.
		{"CREATE TABLE IF NOT EXISTS test.general_log (`event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` mediumtext NOT NULL,`thread_id` bigint(20) unsigned NOT NULL,`server_id` int(10) unsigned NOT NULL,`command_type` varchar(64) NOT NULL,`argument` mediumblob NOT NULL) ENGINE=CSV DEFAULT CHARSET=utf8 COMMENT='General log'", true, "CREATE TABLE IF NOT EXISTS `general_log` (`event_time` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` MEDIUMTEXT NOT NULL,`thread_id` BIGINT(20) UNSIGNED NOT NULL,`server_id` INT(10) UNSIGNED NOT NULL,`command_type` VARCHAR(64) NOT NULL,`argument` MEDIUMBLOB NOT NULL) ENGINE = CSV DEFAULT CHARACTER SET = UTF8 COMMENT = 'General log'"}, //TODO: The number yacc in parentheses has not been implemented yet.
		// For reference_definition in column_definition.
		{"CREATE TABLE test.followers ( f1 int NOT NULL REFERENCES user_profiles (uid) );", true, "CREATE TABLE `followers` (`f1` INT NOT NULL REFERENCES `user_profiles`(`uid`))"},

		// For table option `ENCRYPTION`
		{"create table test.t70 (a int) encryption = 'n';", true, "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'n'"},
		{"create table test.t71 (a int) encryption 'n';", true, "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'n'"},
		{"alter table test.t71 encryption = 'y';", true, "ALTER TABLE `t` ENCRYPTION = 'y'"},
		{"alter table test.t71 encryption 'y';", true, "ALTER TABLE `t` ENCRYPTION = 'y'"},

		// for alter database/schema/table
		{"ALTER DATABASE test CHARACTER SET = 'utf8mb4'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},

		{"ALTER TABLE test.t ADD COLUMN (x SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE test.t ADD COLUMN IF NOT EXISTS (y SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE test.t ADD COLUMN (z SMALLINT UNSIGNED)", true, ""},
		{"ALTER TABLE test.t ADD COLUMN (m SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ALTER TABLE test.t1 ADD COLUMN IF NOT EXISTS (m SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"alter table test.t1 add column (yy int unsigned)", true, ""},
		{"alter table test.t2 add column (yy int unsigned)", true, ""},
		{"alter table test.t4 add column (yy int unsigned)", true, ""},
		{"alter table test.t5 add column (yy int unsigned)", true, ""},
		{"alter table test.t6 add column (yy int unsigned)", true, ""},
		{"alter table test.t7 add column (yy int unsigned)", true, ""},
		{"alter table test.t8 add column (yy int unsigned)", true, ""},
		{"alter table test.t9 add column (yy int unsigned)", true, ""},
		{"alter table test.t10 add column (yy int unsigned)", true, ""},
		{"alter table test.t11 add column (yy int unsigned)", true, ""},
		{"alter table test.t12 add column (yy int unsigned)", true, ""},
		{"alter table test.t13 add column (yy int unsigned)", true, ""},
		{"alter table test.t14 add column (yy int unsigned)", true, ""},
		{"alter table test.t15 add column (yy int unsigned)", true, ""},
		{"alter table test.t16 add column (yy int unsigned)", true, ""},
		{"alter table test.t17 add column (yy int unsigned)", true, ""},
		{"alter table test.t18 add column (yy int unsigned)", true, ""},
		{"alter table test.t19 add column (yy int unsigned)", true, ""},
		{"alter table test.t20 add column (yy int unsigned)", true, ""},
		{"alter table test.t21 add column (yy int unsigned)", true, ""},
		{"alter table test.t22 add column (yy int unsigned)", true, ""},
		{"alter table test.t23 add column (yy int unsigned)", true, ""},
		{"alter table test.t24 add column (yy int unsigned)", true, ""},
		{"alter table test.t25 add column (yy int unsigned)", true, ""},
		{"alter table test.t26 add column (yy int unsigned)", true, ""},
		{"alter table test.t27 add column (yy int unsigned)", true, ""},
		{"alter table test.t28 add column (yy int unsigned)", true, ""},
		{"alter table test.t29 add column (yy int unsigned)", true, ""},
		{"alter table test.t30 add column (yy int unsigned)", true, ""},
		{"alter table test.t1 drop column IF EXISTS yy", true, ""},
		{"alter table test.t5 drop column IF EXISTS yy", true, ""},
		{"alter table test.t7 drop column IF EXISTS yy", true, ""},
		{"alter table test.t9 drop column IF EXISTS yy", true, ""},
		{"alter table test.t11 drop column IF EXISTS yy", true, ""},
		{"alter table test.t13 drop column IF EXISTS yy", true, ""},
		{"alter table test.t15 drop column IF EXISTS yy", true, ""},
		{"alter table test.t17 drop column IF EXISTS yy", true, ""},
		{"alter table test.t19 drop column IF EXISTS yy", true, ""},
		{"alter table test.t21 drop column IF EXISTS yy", true, ""},
		{"alter table test.t23 drop column IF EXISTS yy", true, ""},
		{"alter table test.t25 drop column IF EXISTS yy", true, ""},
		{"alter table test.t27 drop column IF EXISTS yy", true, ""},
		{"alter table test.t29 drop column IF EXISTS yy", true, ""},
	}
	return ddls
}