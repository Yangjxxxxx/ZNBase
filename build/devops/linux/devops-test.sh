#!/bin/bash
set -e
cd $PROJECT_SITE/znbase
#回退前面节点对源码的更改
git reset --hard
git clean -df
#生成test脚本
./build/builder.sh env ZNBASE_NIGHTLY_STRESS=true stdbuf -oL -eL make test TESTTIMEOUT=90m 2>&1