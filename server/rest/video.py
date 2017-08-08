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
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import filtermodel, RestException, \
                            boundHandler, getCurrentUser

from girder.constants import AccessType, TokenScope
from girder.plugins.worker import utils as workerUtils
# from girder.utility.model_importer import ModelImporter

from ..constants import JobStatus


def addItemRoutes(item):
    routes = createRoutes(item)
    item.route('GET', (':id', 'video'), routes['getVideoMetadata'])
    item.route('PUT', (':id', 'video'), routes['processVideo'])
    item.route('DELETE', (':id', 'video'), routes['deleteProcessedVideo'])
    item.route('GET', (':id', 'video', 'frame'), routes['getVideoFrame'])

def createRoutes(item):
    @autoDescribeRoute(
        Description('Return video metadata if it exists.')
        .param('id', 'Id of the item.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the item.', 403)
    )
    @access.public
    @boundHandler(item)
    def getVideoMetadata(self, id, params):
        return {
            'a': 1,
            'b': 2
        }

    @autoDescribeRoute(
        Description('Create a girder-worker job to process the given video.')
        .param('id', 'Id of the item.', paramType='path')
        .param('fileId', 'Id of the file to use as the video.', required=False)
        .param('force', 'Force the creation of a new job.', required=False,
            dataType='boolean', default=False)
        .errorResponse()
        .errorResponse('Read access was denied on the item.', 403)
    )
    @access.public
    @boundHandler(item)
    def processVideo(self, id, params):
        force = params['force']
        user, userToken = getCurrentUser(True)

        itemModel = self.model('item')
        fileModel = self.model('file')
        tokenModel = self.model('token')
        jobModel = self.model('job', 'jobs')

        item = itemModel.load(id, user=user, level=AccessType.READ)

        itemVideoData = item.get('video', {})
        jobId = itemVideoData.get('jobId')

        itemAlreadyProcessed = False
        job = None
        if jobId is not None:
            job = jobModel.load(jobId, level=AccessType.READ, user=user)

        if not force:
            if job is not None:
                status = job['status']
                if status not in (
                        None, JobStatus.ERROR, JobStatus.CANCELED):
                    itemAlreadyProcessed = True

            if itemAlreadyProcessed:
                result = {
                    'video': {
                        'jobCreated': False,
                        'message': 'Processing job already created.'
                    }
                }

                result.update(job)
                return result

        # if user provided fileId, use that one
        fileId = params.get('fileId')
        if fileId is not None:
            # ensure the provided fileId is valid
            inputFile = fileModel.findOne({
                'itemId': ObjectId(id), '_id': ObjectId(fileId)})

            if inputFile is None:
                raise RestException(
                    'Item with id=%s has no such file with id=%s' %
                    (id, fileId))

        else:
            # User did not provide a fileId.
            #
            # If we're *re*running a processing job (force=True), look
            # for the fileId used by the old job.
            if force and job:
                fileId = job.get('meta', {}).get('video', {}).get('fileId')
                if fileId:
                    # ensure the provided fileId is valid, but in this case,
                    # don't raise an exception if it is not -- just discard the
                    # fileId and move on
                    inputFile = fileModel.findOne({
                        'itemId': ObjectId(id), '_id': ObjectId(fileId)})

                    if inputFile is None:
                        fileId = None

        # if we *still* don't have a fileId, just grab the first one found under
        # the given item.
        if fileId is None:
            inputFile = fileModel.findOne({'itemId': ObjectId(id)})

            # if there *are* no files, bail
            if inputFile is None:
                raise RestException('item %s has no files' % itemId)

            fileId = inputFile['_id']

        # if we are *re*running a processing job (force=True), remove all files
        # from this item that were created by the last processing job...
        #
        # ...unless (for some reason) the user is running the job against that
        # particular file (this is almost certainly user error, but for now,
        # we'll just keep the file around).
        if force:
            fileIdList = itemVideoData.get('createdFiles', [])
            for f in fileIdList:
                if f == fileId:
                    continue
                fileModel.remove(fileModel.load(
                    f, level=AccessType.WRITE, user=user))
            itemVideoData['createdFiles'] = []

        # begin construction of the actual job
        if not userToken:
            # It seems like we should be able to use a token without USER_AUTH
            # in its scope, but I'm not sure how.
            userToken = tokenModel.createToken(
                user, days=1, scope=TokenScope.USER_AUTH)

        jobTitle = 'Video Processing'
        job = jobModel.createJob(
            title=jobTitle,
            type='video',
            user=user,
            handler='worker_handler'
        )
        jobToken = jobModel.createJobToken(job)

        job['kwargs'] = job.get('kwargs', {})
        job['kwargs']['task'] = {
            'mode': 'docker',

            # TODO(opadron): replace this once we have a maintained
            #                image on dockerhub
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
                {
                    'id': 'source',
                    'type:': 'string',
                    'format': 'text',
                    'target': 'filepath',
                    'path': '/mnt/girder_worker/data/source.webm'
                },
                {
                    'id': 'meta',
                    'type:': 'string',
                    'format': 'text',
                    'target': 'filepath',
                    'path': '/mnt/girder_worker/data/meta.json'
                },
            ]
        }

        _, itemExt = os.path.splitext(item['name'])

        job['kwargs']['inputs'] = {
            'input': workerUtils.girderInputSpec(
                inputFile,
                resourceType='file',
                token=userToken,
                name='input' + itemExt,
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
                dataFormat='text',
                reference='videoPlugin'
            ),
            '_stderr': workerUtils.girderOutputSpec(
                item,
                parentType='item',
                token=userToken,
                name='processing_stderr.txt',
                dataType='string',
                dataFormat='text',
                reference='videoPlugin'
            ),
            'source': workerUtils.girderOutputSpec(
                item,
                parentType='item',
                token=userToken,
                name='source.webm',
                dataType='string',
                dataFormat='text',
                reference='videoPlugin'
            ),
            'meta': workerUtils.girderOutputSpec(
                item,
                parentType='item',
                token=userToken,
                name='meta.json',
                dataType='string',
                dataFormat='text',
                reference='videoPlugin'
            ),
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

        itemVideoData['jobId'] = str(job['_id'])
        item['video'] = itemVideoData
        itemModel.save(item)

        result = {
            'video': {
                'jobCreated': True,
                'message': 'Processing job created.'
            }
        }

        result.update(job)
        return result


    @autoDescribeRoute(
        Description('Delete the processed results from the given video.')
        .param('id', 'Id of the item.', paramType='path')
        .errorResponse()
        .errorResponse('Write access was denied on the item.', 403)
    )
    @access.public
    @boundHandler(item)
    def deleteProcessedVideo(params):
        pass


    @autoDescribeRoute(
        Description('Get a single frame from the given video.')
        .param('id', 'Id of the item.', paramType='path')
        .param('time', 'Point in time from which to sample the frame.',
               required=True)
        .errorResponse()
        .errorResponse('Read access was denied on the item.', 403)
    )
    @access.public
    @boundHandler(item)
    def getVideoFrame(params):
        pass

    return {
        'getVideoMetadata': getVideoMetadata,
        'processVideo': processVideo,
        'deleteProcessedVideo': deleteProcessedVideo,
        'getVideoFrame': getVideoFrame
    }

