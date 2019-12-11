#!/bin/sh

set -eu

./bin/pitr -data-dir ./tests/test_binlog > pitr.log

cat pitr.log