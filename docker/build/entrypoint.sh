#! /usr/bin/env bash

cd /girder
pip install -e '.[plugins]'
girder-install web --all-plugins
exec girder-install web "$@"

