#!/bin/sh -e
export JEPSEN_ROOT="$(pwd)/projects"
./jepsen/docker/bin/up --dev --node-count 9
