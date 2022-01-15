# Copyright 2017 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# NB: Despite CMake's portability, this build configuration makes no attempt to
# support non-GCC-like compilers.

# The CXX_STANDARD property was introduced in version 3.1
# 3.3 fixes https://cmake.org/cmake/help/v3.3/policy/CMP0060.html
cmake_minimum_required(VERSION 3.3 FATAL_ERROR)

project(plsql)

include_directories (
    include
    ${CMAKE_CURRENT_BINARY_DIR}
    /usr/include/python3.8
#    /usr/include/python3.6m/
)

SET(PARSER_DIR ${CMAKE_SOURCE_DIR}/parser)

ADD_CUSTOM_TARGET(
    GenYaccSource
    DEPENDS ${GEN_SOURCES}
)

SET(GEN_SOURCES
    ${CMAKE_CURRENT_BINARY_DIR}/plsql/pl_gram.c
    ${CMAKE_CURRENT_BINARY_DIR}/include/pl_gram.h
)

add_library(plsql
    mmg/mcxt.c
    mmg/aset.c
    #parser/parser.c
    znbase/znbase.c
    znbase/expr.c
    utils/elog.c
    utils/list.c
    utils/nodes.c
    utils/string.c
    utils/stringinfo.c
    utils/hashmap.c
    utils/numeric.c
    plsql/pl_comp.c
    plsql/pl_exec.c
    plsql/pl_handler.c
    plsql/pl_scanner.c
    plsql/pl_funcs.c
    parser/pl_gram.tab.c
    parser/scan.c
    parser/kwlookup.c
    parser/scansup.c
    utils/decoding.c
    cson/cJSON.c
#   plpython/python_depi
#   plpython/plpy_result.c
#   plpython/plpy_type.c
    utils/bepitime.h utils/bepitime.c)
IF ($ENV{PLPYTHON} MATCHES "true" AND CMAKE_SYSTEM_NAME MATCHES "Linux")
add_library(plpysql
    plpython/python_depi
    plpython/plpy_result.c
    plpython/plpy_type.c
)
link_directories(
       /usr/lib/python3.8/config-3.8-x86_64-linux-gnu
#        /usr/lib/python3.6/config-3.6m-x86_64-linux-gnu/
)
add_library(python3.8 STATIC IMPORTED)
target_link_libraries(
    plsql
    -lpython2.7
    -lpython3.8
    -lpython3.6m
)
ELSE()
add_definitions(-D__APPLE__)
ENDIF()
ADD_DEPENDENCIES(plsql GenYaccSource)

set_target_properties(plsql PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
  COMPILE_OPTIONS "-O0;-g3;-Wall;-Wno-sign-compare"
)

