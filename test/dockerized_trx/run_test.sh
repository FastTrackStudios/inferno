#!/bin/sh -e
pushd ../..
docker build -f Dockerfile.alpine-alsa -t inferno_aoip:alpine-alsa --build-arg BUILD_FLAGS= --build-arg BUILD_TYPE=debug .
docker build -f Dockerfile.fedora-alsa -t inferno_aoip:fedora-alsa --build-arg BUILD_FLAGS= --build-arg BUILD_TYPE=debug .
popd
#docker compose run --rm --build control_and_test
#docker compose down

docker compose down
docker compose up --build
# TODO: will wait infinitely, fix this
