#! /usr/bin/env python

import json

from argparse import ArgumentParser
from time import sleep

from girder.constants import AssetstoreType
from girder_client import GirderClient


def find_user(username):
    result = None
    offset = 0
    while True:
        users = client.get(
            'user',
            parameters=dict(
                text=username,
                limit=50,
                offset=offset,
                sort="login"
            )
        )

        if not users:
            break

        for user in users:
            if user["login"] == username:
                result = user
                break

        if result:
            break

        offset += 50

    return result


def ensure_user(client, **kwds):
    username = kwds['login']
    password = kwds['password']

    user = find_user(username)
    if user:
        client.put(
            'user/{}'.format(user["_id"]),
            parameters=dict(email=kwds['email'],
                            firstName=kwds['firstName'],
                            lastName=kwds['lastName']))

        client.put(
            'user/{}/password'.format(user["_id"]),
            parameters=dict(password=password))
    else:
        client.post('user', parameters=dict(login=username,
                                            password=password,
                                            email=kwds['email'],
                                            firstName=kwds['firstName'],
                                            lastName=kwds['lastName']))

def find_assetstore(name):
    offset = 0
    limit = 50
    result = None
    while result is None:
        assetstore_list = client.get('assetstore',
                                     parameters=dict(limit=str(limit),
                                                     offset=str(offset)))

        if not assetstore_list:
            break

        for assetstore in assetstore_list:
            if assetstore['name'] == name:
                result = assetstore['_id']
                break

        offset += limit

    return result


parser = ArgumentParser(description='Initialize the girder environment')
parser.add_argument('--admin', help='name:pass for the admin user')
parser.add_argument('--user', help='name:pass for the unprivileged user')
parser.add_argument('--host', help='host to connect to')
parser.add_argument('--broker', help='girder worker broker URI')
parser.add_argument('--gridfs-db-name', help='gridfs assetstore db name')

args = parser.parse_args()

apiUrl = args.host + '/api/v1'
client = GirderClient(apiUrl=apiUrl)

a_name, a_pass = args.admin.split(":", 1)
u_name, u_pass = args.user.split(":", 1)

if find_user(a_name):
    client.authenticate(a_name, a_pass)

ensure_user(client,
            login=a_name,
            password=a_pass,
            email='admin@girder.girder',
            firstName='Girder',
            lastName='Admin')

client.authenticate(a_name, a_pass)

ensure_user(client,
            login=u_name,
            password=u_pass,
            email='user@girder.girder',
            firstName='Girder',
            lastName='User')


if find_assetstore('local') is None:
    client.post('assetstore', parameters={
        'name': 'local',
        'db': args.gridfs_db_name,
        'type': str(AssetstoreType.GRIDFS)
    })

client.put('system/plugins', parameters={
    'plugins': json.dumps(['jobs', 'worker', 'video'])
})

client.put('system/restart')

sleep(10)

client.put('system/setting',
           parameters=dict(list=json.dumps([
               dict(key='worker.broker', value=args.broker),
               dict(key='worker.backend', value=args.broker),
               dict(key='worker.api_url', value=apiUrl)])))

client.put('system/restart')

