#!/bin/bash

set -eu

if [ "${1-}" = "shell" ]; then
  shift
  exec /bin/sh "$@"
else
  exec /znbase/znbase "$@"
fi
