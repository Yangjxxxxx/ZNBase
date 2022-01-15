#!/usr/bin/env bash

# This script sanity-checks a source tarball, assuming a Debian-based Linux
# environment with a Go version capable of building ZNBaseDB. Source tarballs
# are expected to build, even after `make clean`, and install a functional
# znbase binary into the PATH, even when the tarball is extracted outside of
# GOPATH.

set -euo pipefail

apt-get update
apt-get install -y autoconf bison cmake libncurses-dev

workdir=$(mktemp -d)
tar xzf znbase.src.tgz -C "$workdir"
(cd "$workdir"/znbase-* && make clean && make install)

znbase start --insecure --store type=mem,size=1GiB --background
znbase sql --insecure <<EOF
  CREATE DATABASE bank;
  CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance DECIMAL);
  INSERT INTO bank.accounts VALUES (1, 1000.50);
EOF
diff -u - <(znbase sql --insecure -e 'SELECT * FROM bank.accounts') <<EOF
id	balance
1	1000.50
EOF
znbase quit --insecure
