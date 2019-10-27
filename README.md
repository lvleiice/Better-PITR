# 基于 TiDB binlog 的 Fast-PITR

通过对 TiDB 的增量备份文件进行预处理，将同一行数据的修改进行合并，生成一个新的更轻量的增量备份文件，从而大大缩短增量备份恢复的时间，实现 Fast-PITR（Fast Point in Time Recovery）。

## 方案

根据互联网特征和 2/8 原则，只有大约 20% 的数据被经常更新。对这一小部分数据的 binlog 数据进行合并可以得到一份非常轻量的差异备份，通过回放这部分少量的备份文件实现 Fast-PITR。如图所示：

![PITR Architecture](/media/program.png)

## 实现

### Map-Reduce 模型

由于需要将同一 key（PK/UK）的所有变更合并到一条 Event 中，需要在内存中维护这个 key 所在行的最新合并数据。如果 binlog 中包含大量不同的 key 的变更，则会占用大量的内存。因此设计了 Map-Reduce 模型来对 binlog 数据进行处理：

![Map Reduce](/media/map_reduce.png)

#### Map

将 binlog 数据按照库名+表名划分到不同的目录下，同时按照 key 的值 hash 到不同的文件中。这样同一行数据的变更都保存在同一文件下，且方便 Reduce 阶段的处理。

#### Reduce

分别对各个表的 binlog 数据进行处理，将同一 key 的数据变更合并到一个 Event 中。合并规则：

| 原 Event 类型 | 新 Event 类型 | 合并后的 Event 类型 |
| :----------- | :----------- | :---------------- |
| INSERT | DELETE |  Nil |
| INSERT | UPDATE | INSERT |
| UPDATE | DELETE | DELETE |
| UPDATE | UPDATE | UPDATE |
| DELETE | INSERT | UPDATE |

由于 Map 阶段将划分的 binlog 数据按照表来保存，因此在 Reduce 阶段很容易地实现了表级别的并发处理。

### DDL 处理

Drainer 输出的 binlog 文件中只包含了各个列的数据，缺乏必要的表结构信息（PK/UK），因此需要获取初始的表结构信息，并且在处理到 DDL binlog 数据时更新表结构信息。DDL 的处理主要实现在 `DDLHandle` 结构中：

![DDL Handle](/media/ddl_handle.png)

首先链接 PD 获取历史 DDL 信息，通过这些历史 DDL 获取 binlog 处理时的初始表结构信息，然后在处理到 DDL binlog 时更新表结构信息。

由于 DDL 的种类比较多，且语法比较复杂，无法在短时间内完成一个完善的 DDL 处理模块，因此使用 [tidb-lite](https://github.com/WangXiangUSTC/tidb-lite) 将 mocktikv 模式的 TiDB 内置到程序中，将 DDL 执行到该 TiDB，再重新获取表结构信息。

## 使用

pitr 提供以下参数：

```bash
> ./bin/pitr --help
Usage of tidb-binlog-pitr:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -V    print pitr version info
  -config string
        [REQUIRED] path to configuration file
  -data-dir string
        drainer data directory path
  -log-file string
        log file path
  -pd-urls string
        a comma separated list of PD endpoints
  -reserve-tmpdir
        reserve temp dir
  -start-datetime string
        recovery from start-datetime, empty string means starting from the beginning of the first file
  -start-tso int
        similar to start-datetime but in pd-server tso format
  -stop-datetime string
        recovery end in stop-datetime, empty string means never end.
  -stop-tso int
        similar to stop-datetime, but in pd-server tso format
```

运行 pitr：

```bash

./bin/pitr --data-dir data.drainer

```
