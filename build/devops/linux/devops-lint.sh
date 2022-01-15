#!/bin/bash
set -e
cd $PROJECT_SITE/znbase
#生成lint静态检查脚本
./build/builder.sh env ZNBASE_LOGIC_TEST_SKIP=true stdbuf -oL -eL make lint TESTTIMEOUT=90m USE_ROCKSDB_ASSERTIONS=1 2>&1