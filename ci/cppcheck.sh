#!/bin/bash

znbase_path=$(pwd)
ci_path="${znbase_path}/ci"
c_dir="c-deps"
check_dir_list=(libroach)
# echo ${ci_path}
for check_dir in ${check_dir_list[@]};
do
	echo ${znbase_path}/${c_dir}/${check_dir}
	cd ${znbase_path}/${c_dir}/${check_dir}
	cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	cppcheck --project=compile_commands.json \
	 --suppressions-list=${ci_path}/suppressions.txt \
	 -igoogletest \
	 --inconclusive \
	 --language=c++ \
	 --std=c++11 \
	 --inline-suppr \
	 --enable=all \
	 -j16 \
	 --template="{file},{line},{severity},{id}" \
	 2>${check_dir}.txt
	cat ${check_dir}.txt | grep "${c_dir}/${check_dir}" | grep -v '.pb.' > ${check_dir}_grep.txt
	if [ `cat ${check_dir}_grep.txt | wc -l` -gt 0 ]; then
		cat ${check_dir}_grep.txt
		exit 2
	fi
done