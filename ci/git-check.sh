#!/usr/bin/env bash

# 忽略无法修复的长度问题
git checkout pkg/util/hlc/timestamp.pb.go

# 忽略生成文件 由于这两个文件为自动生成文件，根据编译环境不同生成的编译文件不同，因此暂时不检查diff
git checkout c-deps/libplsql/parser/pl_gram.tab.c
git checkout c-deps/libplsql/parser/pl_gram.tab.h

if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
    git status >&2 || true
    git diff -a >&2 || true
    exit 1
fi