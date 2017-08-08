#! /usr/bin/env bash

cd /girder
pip install -e '.[plugins]'

if [ "$1" '=' '--post-install' ] ; then
    shift

    pip install girder-client
    echo "RUNNING POST INSTALL"
    python /girder-post-install.py \
        --host "$GIRDER_HOST" \
        --admin "$GIRDER_ADMIN" \
        --gridfs-db-name "$GIRDER_GRIDFS_DB_NAME" \
        --user "$GIRDER_USER" \
        --broker "$GIRDER_BROKER"
    echo "POST INSTALL COMPLETE"

    girder-install web --all-plugins

    echo "SLEEPING"
    while true ; do
        sleep 600
    done
else
    exec girder-install web "$@"
fi

