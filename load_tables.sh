#!/bin/bash
small_table=$1
big_table=$2
node_nums=$3

csv_dir="zipf/csv/${small_table}_${big_table}"
for ((i=1; i <= node_nums; i++))
do
cp ${csv_dir}/*.csv node${i}/extern/self/
done

./znbase sql --insecure --host=:23457 --execute "load table ${small_table}(id INTEGER,val INTEGER) csv data('nodelocal:///self/${small_table}.csv')"
./znbase sql --insecure --host=:23457 --execute "load table ${big_table}(id INTEGER,val INTEGER) csv data('nodelocal:///self/${big_table}.csv')"
./modify_range.sh $small_table 0 65536
./modify_range.sh $big_table 0 65536

