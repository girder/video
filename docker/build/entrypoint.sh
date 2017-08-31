#! /usr/bin/env bash

cd /girder
pip install -e '.[plugins]'

if [ "$1" '=' '--post-install' ] ; then
    shift

    vars="ghost=$GIRDER_HOST"
    vars="$vars gport=$GIRDER_PORT"
    vars="$vars admin_name=$GIRDER_ADMIN_NAME"
    vars="$vars admin_pass=$GIRDER_ADMIN_PASS"
    vars="$vars gridfsDB=$GIRDER_GRIDFS_DB_NAME"
    vars="$vars gridfsHost=$GIRDER_GRIDFS_DB_HOST"
    vars="$vars user_name=$GIRDER_USER_NAME"
    vars="$vars user_pass=$GIRDER_USER_PASS"
    vars="$vars broker=$GIRDER_BROKER"

    export ANSIBLE_LIBRARY=/etc/ansible/roles/girder.girder/library

    echo "RUNNING POST INSTALL"
    ansible-playbook -v --extra-vars "$vars" /girder-post-install.yml
    echo "POST INSTALL COMPLETE"

    girder-install web --all-plugins

    echo "SLEEPING"
    while true ; do
        sleep 600
    done
else
    exec girder-install web "$@"
fi

