#!/bin/bash
set -e
# 编译后校验自动生成代码
if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
  git status >&amp;2 || true
  git diff -a >&amp;2 || true
  exit 1
fi