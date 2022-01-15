#!/bin/bash
set -e
cd $PROJECT_SITE/znbase
#回退前面节点对源码的更改
git reset --hard
git clean -df
#生成testlogic脚本
./build/builder.sh stdbuf -oL -eL make testlogic TESTTIMEOUT=90m 2>&1