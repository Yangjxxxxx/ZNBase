#!/bin/bash
table=$1
min_range=$2
max_range=$3

./znbase sql --insecure --host=:23457 --execute "ALTER TABLE ${table} CONFIGURE ZONE USING range_min_bytes = ${min_range}, range_max_bytes = ${max_range};"

