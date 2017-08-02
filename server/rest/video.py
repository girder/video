#!/usr/bin/env python
# -*- coding: utf-8 -*-

##############################################################################
#  Copyright Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##############################################################################

import os.path

from bson.objectid import ObjectId

from girder import logger
from girder.api import access
from girder.api.describe import describeRoute, Description
from girder.api.rest import filtermodel, RestException, \
                            boundHandler, getCurrentUser

from girder.constants import AccessType, TokenScope
from girder.plugins.worker import utils as workerUtils
# from girder.utility.model_importer import ModelImporter


def addItemRoutes(item):
    item.route('GET', (':id', 'video'), getVideoMetadata)
    item.route('PUT', (':id', 'video'), processVideo)
    item.route('DELETE', (':id', 'video'), deleteProcessedVideo)
    item.route('GET', (':id', 'video', 'frame'), getVideoFrame)


@describeRoute(
    Description('Return video metadata if it exists.')
    .param('id', 'Id of the item.', paramType='path')
    .errorResponse()
    .errorResponse('Read access was denied on the item.', 403)
)
@access.public
@boundHandler
def getVideoMetadata(self, id, params):
    return {
        'a': 1,
        'b': 2
    }


@describeRoute(
    Description('Create a girder-worker job to process the given video.')
    .param('id', 'Id of the item.', paramType='path')
    .param('fileId', 'Id of the file to use as the video.', required=False)
    .errorResponse()
    .errorResponse('Read access was denied on the item.', 403)
)
@access.public
@boundHandler
def processVideo(self, id, params):
    user, userToken = getCurrentUser(True)

    itemModel = self.model('item')
    fileModel = self.model('file')
    tokenModel = self.model('token')
    jobModel = self.model('job', 'jobs')

    fileId = params.get('fileId')
    if fileId is None:
        inputFile = fileModel.findOne({'itemId': ObjectId(id)})
        fileId = inputFile['_id']
    else:
        inputFile = fileModel.findOne({
            'itemId': ObjectId(id), '_id': ObjectId(fileId)})

        if inputFile is None:
            raise RestException(
                'Item with id=%s has no such file with id=%s' % (id, fileId))

    if not userToken:
        # It seems like we should be able to use a token without USER_AUTH
        # in its scope, but I'm not sure how.
        userToken = tokenModel.createToken(
            user, days=1, scope=TokenScope.USER_AUTH)

    item = itemModel.load(id, user=user, level=AccessType.READ)

    itemVideoData = item.get('video', {})
    jobId = itemVideoData.get('jobId')

    if jobId is None:
        jobTitle = 'Video Processing'
        job = jobModel.createJob(
            title=jobTitle, type='video', user=user, handler='worker_handler')
        jobToken = jobModel.createJobToken(job)

        job['kwargs'] = job.get('kwargs', {})
        job['kwargs']['task'] = {
            'mode': 'docker',

            # TODO(opadron): replace this once we have a maintained image on
            #                dockerhub
            'docker_image': 'ffmpeg_local',
            'pull_image': False,
            'inputs': [
                {
                    'id': 'input',
                    'type': 'string',
                    'format': 'text',
                    'target': 'filepath'
                }
            ],
            'outputs': [
                {
                    'id': '_stdout',
                    'type': 'string',
                    'format': 'text',
                    'target': 'memory'
                },
                {
                    'id': '_stderr',
                    'type': 'string',
                    'format': 'text',
                    'target': 'memory'
                },
            ]
        }

        _, itemExt = os.path.splitext(item['name'])

        job['kwargs']['inputs'] = {
            'input': workerUtils.girderInputSpec(
                inputFile,
                resourceType='file',
                token=userToken,
                name='input_file' + itemExt,
                dataType='string',
                dataFormat='text'
            )
        }

        job['kwargs']['outputs'] = {
            '_stdout': workerUtils.girderOutputSpec(
                item,
                parentType='item',
                token=userToken,
                name='processing_stdout.txt',
                dataType='string',
                dataFormat='text'
            ),
            '_stderr': workerUtils.girderOutputSpec(
                item,
                parentType='item',
                token=userToken,
                name='processing_stderr.txt',
                dataType='string',
                dataFormat='text'
            )
        }

        job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
            job=job,
            token=jobToken,
            logPrint=True)

        job['meta'] = job.get('meta', {})
        job['meta']['video_plugin'] = {
            'itemId': id,
            'fileId': fileId
        }

        job = jobModel.save(job)
        jobModel.scheduleJob(job)

        item['video'] = item.get('video', {})
        item['video']['jobId'] = str(job['_id'])
        itemModel.save(item)
    else:
        job = jobModel.load(jobId, level=AccessType.READ, user=user)

    return job


@describeRoute(
    Description('Delete the processed results from the given video.')
    .param('id', 'Id of the item.', paramType='path')
    .errorResponse()
    .errorResponse('Write access was denied on the item.', 403)
)
@access.public
def deleteProcessedVideo(params):
    pass


@describeRoute(
    Description('Get a single frame from the given video.')
    .param('id', 'Id of the item.', paramType='path')
    .param('time', 'Point in time from which to sample the frame.',
           required=True)
    .errorResponse()
    .errorResponse('Read access was denied on the item.', 403)
)
def getVideoFrame(params):
    pass

