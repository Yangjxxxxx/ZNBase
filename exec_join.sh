#!/bin/bash
small_table=$1
big_table=$2
./znbase sql --insecure --host=:23457 --execute "explain analyze(distsql) select * from ${big_table} join ${small_table} on (${big_table}.id = ${small_table}.id);"
