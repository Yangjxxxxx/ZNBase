#!/usr/bin/env bash

# This file builds a znbase binary that's used by integration tests external
# to this repository.

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Build test binary"
run build/builder.sh mkrelease linux-gnu -Otarget
run mv znbase-linux-2.6.32-gnu-amd64 artifacts/znbase
tc_end_block "Build test binary"
