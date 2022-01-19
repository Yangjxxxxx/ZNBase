#!/bin/bash
files="zipf/csv/${1}_${2}/*.csv"
cp $files node1/extern/self
cp $files node2/extern/self
cp $files node3/extern/self

