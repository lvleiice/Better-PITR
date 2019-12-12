#!/bin/sh

set -e

cd "$(dirname "$0")"

down_run_sql "DROP DATABASE IF EXISTS pitr_basic"

rm -rf /tmp/tidb_binlog_pitr_test/data.drainer

GO111MODULE=on go build -o generate_data

run_sql "CREATE DATABASE IF NOT EXISTS \`pitr_basic\`"

echo "generate data in TiDB"
./generate_data -config ./config/generate_data.toml > ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1

echo "use pitr to compress binlog file"

sleep 60

pitr -data-dir $OUT_DIR/drainer > ${OUT_DIR-/tmp}/pitr.log 2>&1

ls -l ./$OUT_DIR/
ls -l ./$OUT_DIR/new_binlog

for data_dir in ./$OUT_DIR/new_binlog; do
    echo "use reparo replay data under ${data_dif}"
    reparo -config ./config/reparo.toml -data-dir ${data_dir} >> ${OUT_DIR-/tmp}/reparo.log 2&>1
done

check_data ./config/sync_diff_inspector.toml 

# clean up
run_sql "DROP DATABASE IF EXISTS \`pitr_basic\`"

killall drainer
