# Docker Deploy

Installing docker is a prerequisite. The instructions differ depending on the
environment. Docker is comprised of two parts: the daemon server which runs on
Linux and accepts commands, and the client which is a Go program capable of
running on MacOS, all Unix variants and Windows.

## Docker Installation

Follow the [Docker install
instructions](https://docs.docker.com/engine/installation/).

## Available images

There are development and deploy images available.

### Development

The development image is a bulky image containing a complete build toolchain.
It is well suited to hacking around and running the tests (including the
acceptance tests). To fetch this image, run `./builder.sh pull`. The image can
be run conveniently via `./builder.sh`.

### Deployment

The deploy image is a downsized image containing a minimal environment for
running ZNBaseDB. It is based on Debian Jessie and contains only the main
ZNBaseDB binary. To fetch this image, run `docker pull
znbasedb/znbase` in the usual fashion.

To build the image yourself, use the Dockerfile in the `deploy` directory after
building a release version of the binary with the development image described in
the previous section. The ZNBaseDB binary will be built inside of that
development container, then placed into the minimal deployment container. The
resulting image `znbasedb/znbase` can be run via `docker run` in the
usual fashion. To be more specific, the steps to do this are:

```
go/src/github.com/znbasedb/znbase $ ./build/builder.sh make build TYPE=release-linux-gnu
go/src/github.com/znbasedb/znbase $ cp ./znbase-linux-2.6.32-gnu-amd64 build/deploy/znbase
go/src/github.com/znbasedb/znbase $ cd build/deploy && docker build -t znbasedb/znbase .
```

# Upgrading / extending the Docker image

Process:

- edit `build/Dockerfile` as desired
- run `build/builder.sh init` to test -- this will build the image locally. Beware this can take a lot of time. The result of `init` is a docker image version which you can subsequently stick into the `version` variable inside the `builder.sh` script for testing locally.
- Once you are happy with the result, run `build/builder.sh push` which pushes your image towards Docker hub, so that it becomes available to others. The result is again a version number, which you then *must* copy back into `builder.sh`. Then commit the change to both Dockerfile and `builder.sh` and submit a PR.

#  Dependencies

A snapshot of ZNBaseDB's dependencies is maintained at
https://github.com/znbasedb/vendored and checked out as a submodule at
`./vendor`.

## Updating Dependencies

This snapshot was built and is managed using `dep` and we manage `vendor` as a
submodule.

Use the version of `dep` in `bin` (may need to `make` first): import your new
dependency from the Go source you're working on, then run `./bin/dep ensure`.

### Working with Submodules

To keep the bloat of all the changes in all our dependencies out of our main
repository, we embed `vendor` as a git submodule, storing its content and
history in [`vendored`](https://github.com/znbasedb/vendored) instead.

This split across two repositories however means that changes involving
changed dependencies require a two step process.

- After using dep to add or update dependencies and making related code
changes, `git status` in `znbasedb/znbase` checkout will report that the
`vendor` submodule has `modified/untracked content`.

- Switch into `vendor` and commit all changes (or use `git -C vendor`), on a
new named branch.

   + At this point the `git status` in your `znbasedb/znbase` checkout
will report `new commits` for `vendor` instead of `modified content`.

- Commit your code changes and new `vendor` submodule ref.

- Before this commit can be submitted in a pull request to
`znbasedb/znbase`, the submodule commit it references must be available
on `github.com/znbasedb/vendored`.

* Organization members can push their named branches there directly.

* Non-members should fork the `vendored` repo and submit a pull request to
`znbasedb/vendored`, and need wait for it to merge before they will be able
to use it in a `znbasedb/znbase` PR.

#### `master` Branch Pointer in Vendored Repo

Since the `znbasedb/znbase` submodule references individual commit
hashes in `vendored`, there is little significance to the `master` branch in
`vendored` -- as outlined above, new commits are always authored with the
previously referenced commit as their parent, regardless of what `master`
happens to be.

That said, it is critical that any ref in `vendored` that is referenced from
`znbasedb/znbase` remain available in `vendored` in perpetuity: after a
PR referencing a ref merges, the `vendored` `master` branch should be updated
to point to it before the named feature branch can be deleted, to ensure the
ref remains reachable and thus is never garbage collected.

#### Conflicting Submodule Changes

The canonical linearization of history is always the main repo. In the event
of concurrent changes to `vendor`, the first should cause the second to see a
conflict on the `vendor` submodule pointer. When resolving that conflict, it
is important to re-run dep against the fetched, updated `vendor` ref, thus
generating a new commit in the submodule that has as its parent the one from
the earlier change.

## Repository Name

We only want the vendor directory used by builds when it is explicitly checked
out *and managed* as a submodule at `./vendor`.

If a go build fails to find a dependency in `./vendor`, it will continue
searching anything named "vendor" in parent directories. Thus the vendor
repository is _not_ named "vendor", to minimize the risk of it ending up
somewhere in `GOPATH` with the name `vendor` (e.g. if it is manually cloned),
where it could end up being unintentionally used by builds and causing
confusion.
