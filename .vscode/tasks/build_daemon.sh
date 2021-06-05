#!/bin/bash

set -ex

echo "Building daemon"

rm -rf kachery_daemon/*.tgz
rm -rf daemon/*.tgz
cd daemon
yarn install
yarn build
npm pack
cp kachery-daemon-*.tgz ../kachery_daemon/