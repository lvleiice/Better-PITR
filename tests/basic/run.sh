#!/bin/sh

set -e

cd "$(dirname "$0")"

down_run_sql "DROP DATABASE IF EXISTS pitr_basic"

rm -rf /tmp/tidb_binlog_pitr_test/drainer/

GO111MODULE=on go build -o generate_data

run_sql "CREATE DATABASE IF NOT EXISTS \`pitr_basic\`"

echo "generate data in TiDB"
./generate_data -config ./config/generate_data.toml > ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1

echo "use pitr to compress binlog file"
ls -l /$OUT_DIR/drainer || true
pitr -data-dir $OUT_DIR/drainer -output-dir $OUT_DIR/new_binlog > ${OUT_DIR-/tmp}/pitr.log 2>&1

ls -l /$OUT_DIR/ || true
ls -l /$OUT_DIR/new_binlog || true

for data_dir in `ls /$OUT_DIR/new_binlog`; do
    echo "use reparo replay data under ${data_dir}"
    reparo -config ./config/reparo.toml -data-dir /$OUT_DIR/new_binlog/${data_dir} >> ${OUT_DIR-/tmp}/reparo.log 2>&1
done

check_data ./config/sync_diff_inspector.toml 

# clean up
run_sql "DROP DATABASE IF EXISTS \`pitr_basic\`"

killall drainer
