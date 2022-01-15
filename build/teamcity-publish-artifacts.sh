#!/usr/bin/env bash

# Any arguments to this script are passed through unmodified to
# ./build/teamcity-publish-s3-binaries.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/teamcity-publish-s3-binaries.sh "$@"

# When publishing a release, build and publish a docker image.
if [ "$TEAMCITY_BUILDCONF_NAME" == 'Publish Releases' ]; then
  # Unstable releases go to a special znbase-unstable image name, while
  # stable releases go to the official znbasedb/znbase image name.
  # According to semver rules, non-final releases can be distinguished
  # by the presence of a hyphen in the version number.
  image=docker.io/znbasedb/znbase-unstable
  if [[ "$TC_BUILD_BRANCH" != *-* ]]; then
    image=docker.io/znbasedb/znbase
  fi

  cp znbase.linux-2.6.32-gnu-amd64 build/deploy/znbase
  docker build --no-cache --tag=$image:{latest,"$TC_BUILD_BRANCH"} build/deploy

  TYPE=$(go env GOOS)

  # For the acceptance tests that run without Docker.
  ln -s znbase.linux-2.6.32-gnu-amd64 znbase
  build/builder.sh mkrelease $TYPE testbuild TAGS=acceptance PKG=./pkg/acceptance
  (cd pkg/acceptance && ./acceptance.test -l ./artifacts -i $image -b /znbase/znbase -test.v -test.timeout -5m) &> ./artifacts/publish-acceptance.log

  sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg
  docker push "$image:latest"
  docker push "$image:$TC_BUILD_BRANCH"
fi
