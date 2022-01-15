#!/usr/bin/env bash

wget --http-user=admin --http-password=123456a?Wp http://repo.inspur.com/artifactory/newsql-binary/latest-commit

latest=`cat latest-commit`
now=`git rev-parse HEAD`

count=`git diff --name-only ${latest} ${now} | grep c-deps | wc -l`

if [ ${count} -eq 0 ]; then
    wget --http-user=admin --http-password=123456a?Wp http://repo.inspur.com/artifactory/newsql-binary/native.tar
    tar -xvf native.tar -C /
    make C_COMPILE_FLAG=false -Otarget c-deps
    make C_COMPILE_FLAG=false BUILDTYPE=release XGOOS=linux XGOARCH=amd64 XCMAKE_SYSTEM_NAME=Linux TARGET_TRIPLE=x86_64-unknown-linux-musl LDFLAGS=-static SUFFIX=-linux-2.6.32-musl-amd64
else
    make C_COMPILE_FLAG=true -Otarget c-deps
    make C_COMPILE_FLAG=true BUILDTYPE=release XGOOS=linux XGOARCH=amd64 XCMAKE_SYSTEM_NAME=Linux TARGET_TRIPLE=x86_64-unknown-linux-musl LDFLAGS=-static SUFFIX=-linux-2.6.32-musl-amd64
    echo ${now} > latest-commit
    tar -cvf native.tar /go/native
    curl -u admin:123456a?Wp -X PUT http://repo.inspur.com/artifactory/newsql-binary/native.tar -T native.tar
    curl -u admin:123456a?Wp -X PUT http://repo.inspur.com/artifactory/newsql-binary/latest-commit -T latest-commit
fi
