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

import json
import os.path

from functools import wraps

from cherrypy import HTTPRedirect

from bson.objectid import ObjectId

from girder import logger
from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import filtermodel, RestException, Resource, \
                            boundHandler, getCurrentUser

from girder.constants import AccessType, TokenScope
from girder.plugins.worker import utils as workerUtils
from girder.models.model_base import ValidationException

from ..constants import JobStatus, VideoEnum, VideoEnum


### BEGIN HELPER FUNCTIONS     TODO - Refactor
def getFile(self, id, user=None, level=None):
    if user is None:
        user = getCurrentUser()

    if level is None:
        level = AccessType.READ

    result = self.model('file').load(id, user=user, level=level)
    return result


def getVideoData(
        self, fileId=None, formatId=None,
        user=None, ensure=False):
    videoModel = self.model('video', 'video')

    if user is None:
        user = getCurrentUser()

    data = None
    if formatId is None:
        data = videoModel.findOne({
            'type': VideoEnum.FILE,
            'fileId': fileId,
            'sourceFileId': fileId
        })

        if not data and ensure:
            data = videoModel.createVideoFile(fileId=fileId)
    else:
        data = videoModel.findOne({
            'type': VideoEnum.FILE,
            'sourceFileId': fileId,
            'formatId': formatId
        })

        if not data and ensure:
            data = videoModel.createVideoFile(
                sourceFileId=fileId,
                formatId=formatId)

    return data


def cancelVideoJobs(
        self, id, formatId=None, user=None, mask=None, cascade=True):
    videoModel = self.model('video', 'video')
    jobModel = self.model('job', 'jobs')

    _id = ObjectId(id)

    if not user:
        user = getCurrentUser()

    if mask is None:
        mask = (VideoEnum.ANALYSIS |
                VideoEnum.TRANSCODING |
                VideoEnum.FRAME_EXTRACTION)

    isJob = { 'type': VideoEnum.JOB }

    isCorrectJobType = [
        { 'jobType': jobType }
        for jobType in (
            VideoEnum.ANALYSIS,
            VideoEnum.TRANSCODING,
            VideoEnum.FRAME_EXTRACTION)
        if jobType & mask
    ]

    isCorrectJobType = (
        { 'type': 0xFFFF }  # should not match anything
        if len(isCorrectJobType) == 0 else

        isCorrectJobType[0]
        if len(isCorrectJobType) == 1 else

        { '$or': isCorrectJobType }
    )

    derivedFromSource = { 'sourceFileId': _id }
    matchesSource = { 'fileId': _id }
    notMatchesSource = { 'fileId': { '$ne' : _id } }

    isCorrectId = (
        { '$and' : [derivedFromSource, notMatchesSource] }
        if formatId else

        derivedFromSource
        if cascade else

        matchesSource
    )

    query = [isJob, isCorrectJobType, isCorrectId]
    if not cascade:
        query.append({ 'formatId': formatId })

    canceledJobs = set()

    for vJob in videoModel.find({ '$and': query }):
        job = jobModel.load(
            vJob['jobId'], level=AccessType.WRITE, user=user)

        try:
            jobModel.cancelJob(job)
            canceledJobs.add(str(job['_id']))
        except ValidationException:
            pass

        videoModel.remove(vJob)

    return {
        'message': 'Video processing jobs canceled.',
        'fileId': id,
        'canceledJobs': list(canceledJobs)
    }


def removeVideoData(
        self, id, formatId=None, user=None, mask=None, cascade=True):
    fileModel = self.model('file')
    videoModel = self.model('video', 'video')

    _id = ObjectId(id)

    if not user:
        user = getCurrentUser()

    if mask is None:
        mask = (VideoEnum.FILE | VideoEnum.FRAME)

    isCorrectType = [
        { 'type': type }
        for type in (VideoEnum.FILE, VideoEnum.FRAME)
        if type & mask
    ]

    isCorrectType = (
        { 'type': 0xFFFF }  # should not match anything
        if len(isCorrectType) == 0 else

        isCorrectType[0]
        if len(isCorrectType) == 1 else

        { '$or': isCorrectType }
    )

    derivedFromSource = { 'sourceFileId': _id }
    matchesSource = { 'fileId': _id }
    notMatchesSource = { 'fileId': { '$ne' : _id } }

    isCorrectId = (
        { '$and' : [derivedFromSource, notMatchesSource] }
        if formatId else

        derivedFromSource
        if cascade else

        matchesSource
    )

    query = [isCorrectType, isCorrectId]
    if not cascade:
        query.append({ 'formatId': formatId })

    deletedFiles = set()

    for vData in videoModel.find({ '$and': query }):
        file = fileModel.load(
            vData['fileId'], level=AccessType.WRITE, user=user)

        videoModel.remove(vData)

        try:
            fileModel.remove(file)
            deletedFiles.add(str(file['_id']))
        except ValidationException:
            pass

    return {
        'message': 'Processed video data deleted.',
        'fileId': id,
        'deletedFiles': list(deletedFiles)
    }


def analyzeVideo(self, fileId, force=False, formatId=None, user=None):
    userToken = None

    if user is None:
        user, userToken = getCurrentUser(True)

    itemModel = self.model('item')
    tokenModel = self.model('token')
    jobModel = self.model('job', 'jobs')
    videoModel = self.model('video', 'video')

    file = getFile(self, fileId, user=user)

    vData = getVideoData(
        self, fileId=fileId, formatId=formatId, user=user, ensure=True)

    vJob = videoModel.findOne({
        'type': VideoEnum.JOB,
        'jobType': VideoEnum.ANALYSIS,
        'fileId': vData['fileId'],
        'sourceFileId': vData['sourceFileId']
    })

    jobId = (vJob or {}).get('jobId')

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

    # if we are *re*running a processing job (force=True), remove all data
    # for this video that were created by the last processing jobs
    if force:
        cancelVideoJobs(
            self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.ANALYSIS, cascade=False)

    # begin construction of the actual job
    if not userToken:
        userToken = tokenModel.createToken(
            user, days=1, scope=TokenScope.USER_AUTH)

    stash = itemModel.createHiddenItem(user)
    stash = itemModel.load(stash['_id'], level=AccessType.WRITE, user=user)
    targetFile = getFile(self, vData['fileId'], user=user)

    jobTitle = '[video] Analysis'
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
        'container_args': ['analyze'],
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
                'id': 'meta',
                'type:': 'string',
                'format': 'text',
                'target': 'filepath',
                'path': '/mnt/girder_worker/data/meta.json'
            },
        ]
    }

    _, itemExt = os.path.splitext(targetFile['name'])

    job['kwargs']['inputs'] = {
        'input': workerUtils.girderInputSpec(
            targetFile,
            resourceType='file',
            token=userToken,
            name='input' + itemExt,
            dataType='string',
            dataFormat='text'
        )
    }


    job['kwargs']['outputs'] = {
        'meta': workerUtils.girderOutputSpec(
            stash,
            parentType='item',
            token=userToken,
            name='meta.json',
            dataType='string',
            dataFormat='text',
            reference=':'.join(('override', json.dumps(dict(
                attachParent=True,
                # assetstore=None, TODO(opadron)
                reference=':'.join(('videoPlugin', json.dumps(dict(
                    fileId=str(fileId),
                    formatId=str(formatId) if formatId else None,
                    dataType='analysis',
                ))))
            ))))
        )
    }

    job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
        job=job,
        token=jobToken,
        logPrint=True)

    job = jobModel.save(job)
    jobModel.scheduleJob(job)

    videoModel.createVideoJob(
        fileId=vData['fileId'],
        sourceFileId=vData['sourceFileId'],
        formatId=formatId,
        jobType=VideoEnum.ANALYSIS,
        jobId=job['_id'],
        save=True)

    result = {
        'video': {
            'jobCreated': True,
            'message': 'Processing job created.'
        }
    }

    result.update(job)
    return result


def transcodeVideo(self, fileId, formatId, force=False, user=None):
    userToken = None

    if user is None:
        user, userToken = getCurrentUser(True)

    itemModel = self.model('item')
    tokenModel = self.model('token')
    jobModel = self.model('job', 'jobs')
    videoModel = self.model('video', 'video')

    file = getFile(self, fileId)
    originalFileName, _ = os.path.splitext(file['name'])

    formatName = None
    if formatId:
        format = videoModel.findOne({
            'type': VideoEnum.FORMAT,
            '_id': ObjectId(formatId)
        })

        formatName = format['name']
        formatId = format['_id']

    fData = videoModel.findOne({
        'type': VideoEnum.FORMAT,
        '_id': formatId
    })

    videoCodec = fData.get('videoCodec')
    audioCodec = fData.get('audioCodec')

    width = fData.get('videoWidth')
    height = fData.get('videoHeight')

    dimensions = None
    if width and height:
        dimensions = '%dx%d' % (width, height)

    audioSampleRate = fData.get('audioSampleRate')
    audioBitRate = fData.get('audioBitRate')
    videoBitRate = fData.get('videoBitRate')

    vData = getVideoData(
        self, fileId=fileId, formatId=formatId, user=user, ensure=True)

    vJob = videoModel.findOne({
        'type': VideoEnum.JOB,
        'jobType': VideoEnum.TRANSCODING,
        'sourceFileId': vData['sourceFileId'],
        'formatId': formatId
    })

    jobId = (vJob or {}).get('jobId')

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

    # if we are *re*running a processing job (force=True), remove all data
    # for this video that were created by the last processing jobs
    if force:
        cancelVideoJobs(self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.TRANSCODING | VideoEnum.FRAME_EXTRACTION,
            cascade=False)

        removeVideoData(self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.FILE, cascade=False)

    # begin construction of the actual job
    if not userToken:
        userToken = tokenModel.createToken(
            user, days=1, scope=TokenScope.USER_AUTH)

    stash = itemModel.createHiddenItem(user)
    stash = itemModel.load(stash['_id'], level=AccessType.WRITE, user=user)

    jobTitle = '[video] Transcoding'
    job = jobModel.createJob(
        title=jobTitle,
        type='video',
        user=user,
        handler='worker_handler'
    )
    jobToken = jobModel.createJobToken(job)

    container_args = ['transcode']

    for code, val in (('VCODEC', videoCodec),
                      ('ACODEC', audioCodec),
                      ('DIMENSIONS', dimensions),
                      ('ASRATE', audioSampleRate),
                      ('ABRATE', audioBitRate),
                      ('VBRATE', videoBitRate)):
        if val:
            container_args.extend([code, val])

    # TODO(opadron): add more options
    fileExtension = None
    if videoCodec == 'VP9':
        fileExtension = 'webm'
    else:
        fileExtension = 'webm'

    job['kwargs'] = job.get('kwargs', {})
    job['kwargs']['task'] = {
        'mode': 'docker',

        # TODO(opadron): replace this once we have a maintained
        #                image on dockerhub
        'docker_image': 'ffmpeg_local',
        'pull_image': False,
        'container_args': container_args + [fileExtension],
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
                'id': 'output',
                'type:': 'string',
                'format': 'text',
                'target': 'filepath',
                'path': '/mnt/girder_worker/data/output.%s' % fileExtension
            },
        ]
    }

    _, itemExt = os.path.splitext(file['name'])

    job['kwargs']['inputs'] = {
        'input': workerUtils.girderInputSpec(
            file,
            resourceType='file',
            token=userToken,
            name='input' + itemExt,
            dataType='string',
            dataFormat='text'
        )
    }

    job['kwargs']['outputs'] = {
        'output': workerUtils.girderOutputSpec(
            stash,
            parentType='item',
            token=userToken,
            name='%s_%s.%s' % (originalFileName, formatName, fileExtension),
            dataType='string',
            dataFormat='text',
            reference=':'.join(('override', json.dumps(dict(
                attachParent=True,
                # assetstore=None, TODO(opadron)
                reference=':'.join(('videoPlugin', json.dumps(dict(
                    fileId=str(fileId),
                    formatId=str(formatId) if formatId else None,
                    dataType='transcoding'
                ))))
            ))))
        )
    }

    job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
        job=job,
        token=jobToken,
        logPrint=True)

    job = jobModel.save(job)
    jobModel.scheduleJob(job)

    videoModel.createVideoJob(
        fileId=fileId,
        sourceFileId=fileId,
        formatId=formatId,
        jobType=VideoEnum.TRANSCODING,
        jobId=job['_id'],
        save=True)

    videoModel.save(vData)

    result = {
        'video': {
            'jobCreated': True,
            'message': 'Processing job created.'
        }
    }

    result.update(job)
    return result


def extractFrames(
        self, fileId, numFrames, formatId=None, user=None, force=False):
    userToken = None

    if user is None:
        user, userToken = getCurrentUser(True)

    itemModel = self.model('item')
    tokenModel = self.model('token')
    jobModel = self.model('job', 'jobs')
    videoModel = self.model('video', 'video')

    file = getFile(self, fileId, user=user)
    vData = getVideoData(
        self, fileId=fileId, formatId=formatId, user=user)

    inputFile = file
    if formatId:
        inputFile = getFile(self, vData['fileId'], user=user)

    vJob = videoModel.findOne({
        'type': VideoEnum.JOB,
        'jobType': VideoEnum.FRAME_EXTRACTION,
        'sourceFileId': vData['sourceFileId'],
        'formatId': formatId
    })

    jobId = (vJob or {}).get('jobId')

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

    # if we are *re*running a processing job (force=True), remove all data
    # for this video that were created by the last processing jobs
    if force:
        cancelVideoJobs(self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.FRAME_EXTRACTION, cascade=False)

        removeVideoData(self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.FRAME, cascade=False)

    if not userToken:
        userToken = tokenModel.createToken(
            user, days=1, scope=TokenScope.USER_AUTH)

    stash = itemModel.createHiddenItem(user)
    stash = itemModel.load(stash['_id'], level=AccessType.WRITE, user=user)

    jobTitle = '[video] Frame Extraction'
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
        'container_args': ['extract', str(numFrames)],
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
                'id': 'frame%d' % index,
                'type:': 'string',
                'format': 'text',
                'target': 'filepath',
                'path': '/mnt/girder_worker/data/%d.png' % (index + 1)
            }
            for index in range(numFrames)
        ]
    }

    _, itemExt = os.path.splitext(inputFile['name'])

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

    job['kwargs']['outputs'] = dict(
        (
            'frame%d' % index,
            workerUtils.girderOutputSpec(
                stash,
                parentType='item',
                token=userToken,
                name='%d.png' % index,
                dataType='string',
                dataFormat='text',
                reference=':'.join(('override', json.dumps(dict(
                    attachParent=True,
                    # assetstore=None, TODO(opadron)
                    reference=':'.join(('videoPlugin', json.dumps(dict(
                        fileId=str(fileId),
                        formatId=str(formatId) if formatId else None,
                        dataType='frame',
                        index=index
                    ))))
                ))))
            )
        )
        for index in range(numFrames)
    )

    job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
        job=job,
        token=jobToken,
        logPrint=True)

    job = jobModel.save(job)
    jobModel.scheduleJob(job)

    videoModel.createVideoJob(
        fileId=inputFile['_id'],
        sourceFileId=fileId,
        formatId=formatId,
        jobType=VideoEnum.FRAME_EXTRACTION,
        jobId=job['_id'],
        save=True)

    result = {
        'video': {
            'jobCreated': True,
            'message': 'Frame extraction job created.'
        }
    }

    result.update(job)
    return result

def _getVideoFormats(self, id=None):
    videoModel = self.model('video', 'video')

    query = { 'type': VideoEnum.FORMAT }

    if id:
        formatIds = set()
        for vData in videoModel.find({'sourceFileId': id,
                                      'type': VideoEnum.FILE}):
            if vData['formatId']:
                formatIds.add(vData['formatId'])

        correctId = { '$or': [{'_id': _id} for _id in formatIds] }
        query = { '$and': [query, correctId] }

    return list(videoModel.find(query))

def _getVideoFormatByName(self, name, id=None):
    videoModel = self.model('video', 'video')

    format = videoModel.findOne({
        'type': VideoEnum.FORMAT,
        'name': name
    })

    result = None

    if id:
        query = {
            'sourceFileId': id,
            'type': VideoEnum.FILE,
            'formatId': format['_id']
        }

        if videoModel.findOne(query):
            result = format
    else:
        result = format

    if result:
        return result

    raise RestException('No such video format', 404)
### END HELPER FUNCTIONS     TODO - Refactor


class Video(Resource):
    """Video API route class."""

    def __init__(self):
        super(Video, self).__init__()
        self.resourceName = 'video'
        self.route('GET', ('format',), self.getFormats)
        self.route('GET', ('format', ':name'), self.getFormatByName)
        self.route('POST', ('format',), self.createFormat)
        self.route('DELETE', ('format', ':name'), self.deleteFormatByName)

    @autoDescribeRoute(
        Description('Return a list of defined video formats.'))
    @access.public
    def getFormats(self, params):
        return _getVideoFormats(self)

    @autoDescribeRoute(
        Description('Look up a defined video format by name.')
        .param('name', 'Name of the defined video fromat', paramType='path',
               required=True)
        .errorResponse()
        .errorResponse('No such video format.', 404)
    )
    @access.public(scope=TokenScope.DATA_READ)
    def getFormatByName(self, name, params):
        return _getVideoFormatByName(self, name)

    @autoDescribeRoute(
        Description('Create a new video format.')
        .param('name', 'Name of the new format.', required=True)
        .param('dimensions', 'Dimensions of the video.', required=False)
        .param('videoCodec', 'Video codec for the video.',
            required=False, enum=['VP9'], default='VP9')
        .param('audioCodec', 'Audio codec for the video.',
            required=False, enum=['OPUS'], default='OPUS')
        .param(
            'audioSampleRate',
            'Sampling rate for the video audio.',
            required=False)
        .param('audioBitRate', 'Bit rate for the video audio.', required=False)
        .param('videoBitRate', 'Bit rate for the video.', required=False)
        .errorResponse()
        .errorResponse('Format with the given name already exists.', 400)
        .errorResponse('Admin access denied.', 403)
    )
    @access.admin
    def createFormat(self, params):
        videoModel = self.model('video', 'video')
        name = params.pop('name', None)

        if videoModel.findOne({ 'type': VideoEnum.FORMAT, 'name': name }):
            raise RestException(
                'Format with the given name already exists.', 400)

        params.pop('save', None)
        dims = params.pop('dimensions', None)
        if dims:
            w, h = tuple(int(x) for x in dims.split('x'))
            params.update({ 'videoHeight': h, 'videoWidth': w })

        return videoModel.createVideoFormat(
            name=name, save=True, **params)

    @autoDescribeRoute(
        Description('Delete a video format.')
        .param('name', 'Name of the format.', paramType='path', required=True)
        .errorResponse()
        .errorResponse('Format with the given name does not exist.', 404)
        .errorResponse('Admin access denied.', 403)
    )
    @access.admin
    def deleteFormatByName(self, name, params):
        videoModel = self.model('video', 'video')

        vData = videoModel.findOne({
            'type': VideoEnum.FORMAT, 'name': name })

        if not vData:
            raise RestException(
                'Format with the given name does not exist.', 404)

        # TODO(opadron): Go through all video entries and files referencing
        #                this format and remove them, too!

        videoModel.remove(vData)


def addFileRoutes(file):
    routeHandlers = createRoutes(file)
    routeTable = (
        (
            'GET', (
                ((':id', 'video'),                    'getVideo'),
                ((':id', 'video', 'info'),            'fetchVideoData'),
                ((':id', 'video', 'frame'),           'getVideoFrame'),
            )
        ),
        (
            'POST', (
                ((':id', 'video'),                    'processVideo'),
                ((':id', 'video', 'transcode'),       'convertVideo'),
            )
        ),
        (
            'DELETE', (
                ((':id', 'video'),                    'deleteProcessedVideo'),
            )
        ),
    )

    for method, routes in routeTable:
        for route, key in routes:
            file.route(method, route, routeHandlers[key])

    return routeHandlers


def createRoutes(file):
    @autoDescribeRoute(
        Description('Return video stream.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format', 'Format of the video.', required=False)
        .param('offset', 'Forwarded to file/:id/download',
               dataType='integer', required=False, default=0)
        .param('endByte', 'Forwarded to file/:id/download',
               dataType='integer', required=False)
        .param('contentDisposition', 'Forwarded to file/:id/download',
               required=False, enum=['inline', 'attachment'],
               default='attachment')
        .param('extraParameters', 'Forwarded to file/:id/download',
               required=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
        .errorResponse('No such video format.', 404)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def getVideo(self, id, params):
        format = params.get('format')

        id = ObjectId(id)

        formatId = None
        if format:
            formatId = _getVideoFormatByName(self, format)['_id']

        sourceFile = getFile(self, id)

        vData = getVideoData(
            self, fileId=id, formatId=formatId, ensure=False)

        if not vData:
            msg = 'No source video found'
            if format:
                msg = (
                    "Source video has not been "
                    "transcoded to format: '{}'".format(format))

            raise RestException(msg, 404)

        targetFile = getFile(self, vData['fileId'])

        return self.download(id=str(targetFile['_id']), params=dict(
            item for item in params.items() if item[1] is not None
        ))


    @autoDescribeRoute(
        Description('Return the data for the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format',
               'Name of the format, or leave blank for the source video.',
               required=False)
        .errorResponse()
        .errorResponse('Format not found.', 404)
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def fetchVideoData(self, id, params):
        file = getFile(self, id)

        format = params.get('format')
        formatId = None
        if format:
            formatId = _getVideoFormatByName(format)['_id']

        result = getVideoData(self, fileId=file['_id'], formatId=formatId)
        result.pop('_id', None)
        result.pop('fileId', None)
        result.pop('sourceFileId', None)
        result.pop('formatId', None)
        result.pop('type', None)
        return result


    @autoDescribeRoute(
        Description('Return a specific video frame.')
        .notes('This endpoint also accepts the HTTP "Range" '
               'header for partial file downloads.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format',
            'Name of the alternate video format.', required=False)
        .param('percent',
            'Portion of the video as a percentage (0-100).', required=False)
        .param('time',
            'Timestamp of the video in HH:MM:SS format.', required=False)
        .param('index', 'Exact index of the desired frame.',
            dataType='integer', required=False)
        .param('offset', 'Forwarded to file/:id/download',
               dataType='integer', required=False, default=0)
        .param('endByte', 'Forwarded to file/:id/download',
               dataType='integer', required=False)
        .param('contentDisposition', 'Forwarded to file/:id/download',
               required=False, enum=['inline', 'attachment'],
               default='attachment')
        .param('extraParameters', 'Forwarded to file/:id/download',
               required=False)
        .errorResponse()
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied on the parent folder.', 403)
        .errorResponse('No such video format.', 404)
        .errorResponse(
            'Missing metadata from format.', 400)
        .errorResponse(
            'Unrecognized timestamp format.', 400)
        .errorResponse(
            'Number of provided mutually-exclusive options != 1.', 400)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def getVideoFrame(self, id, params):
        format = params.get('format')
        percent = params.get('percent')
        time = params.get('time')
        index = params.get('index')

        fileModel = self.model('file')
        videoModel = self.model('video', 'video')

        id = ObjectId(id)

        count = sum(x is not None for x in (percent, time, index))

        if count == 0 or count > 1:
            raise RestException(
                'Must provide no more than one of either '
                'percent, time, or index')

        formatId = None
        if format:
            formatId = _getVideoFormatByName(self, format)['_id']

        vData = getVideoData(
            self, fileId=id, formatId=formatId)

        if not vData:
            raise RestException('Video data not found', 404)

        frameCount = None
        duration = None
        if percent is not None or time is not None:
            frameCount = vData.get('videoFrameCount', 0)

            if not frameCount:
                raise RestException(
                    'Missing data from video: videoFrameCount')

            frameCount = int(frameCount)

            if time is not None:
                duration = vData.get('duration')
                if duration is None:
                    raise RestException(
                        'Missing data from video: duration')

                duration = float(duration)

        if percent is not None:
            percent = int(percent)

            if percent < 0:
                percent = 0

            if percent > 100:
                percent = 100

            ONE_E_14 =   100000000000000
            ONE_E_16 = 10000000000000000
            index = ONE_E_14 * percent * int(frameCount) // ONE_E_16

            # pass through

        if time is not None:
            tokens = [float(x) for x in time.split(':')]

            time = tokens.pop(0)  # ss

            if tokens:  # mm:ss
                time *= 60.0
                time += tokens.pop(0)

            if tokens:  # hh:mm:ss
                time *= 60.0
                time += tokens.pop(0)

            if tokens:  # dd:hh:mm:ss
                time *= 24.0
                time += tokens.pop(0)

            if tokens:  # unrecognized format
                raise RestException('Unrecognized timestamp format.')

            if time < 0:
                time = 0

            if time > duration:
                time = duration

            index = int(time * frameCount / duration)

            # pass through

        index = int(index)

        query = {
            'type': VideoEnum.FRAME,
            'sourceFileId': id,
            'formatId': formatId,
            'index': index
        }

        frame = videoModel.findOne(query)
        if frame:
            return self.download(id=str(frame['fileId']), params=dict(
                item for item in params.items() if item[1] is not None
            ))

        raise RestException('Frame not found.' + ' ' + str(query), 404)


    @autoDescribeRoute(
        Description('Create a girder-worker job to process the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('force', 'Force the creation of a new job.', required=False,
            dataType='boolean', default=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def processVideo(self, id, params):
        return analyzeVideo(self, id, params.get('force'))


    @autoDescribeRoute(
        Description('Delete the processed results from the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .errorResponse()
        .errorResponse('Write access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteProcessedVideo(self, id, params):
        cancelResponse = cancelVideoJobs(self, id)
        removeResponse = removeVideoData(self, id)

        cancelResponse.pop('message', None)
        removeResponse.pop('message', None)
        removeResponse.pop('fileId', None)

        response = {}
        response.update(cancelResponse)
        response.update(removeResponse)
        response['message'] = 'Processed video data deleted.'

        return response


    @autoDescribeRoute(
        Description('Transcode a video into a new format.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format', 'Name of the format.', required=True)
        .param('force', 'Force the creation of a new job.', required=False,
            dataType='boolean', default=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def convertVideo(self, id, format, params):
        formatId = _getVideoFormatByName(self, format)['_id']
        return transcodeVideo(
            self, id, formatId, force=params.get('force', False))


    def wrap(fun):
        @wraps(fun)
        def result(*args, **kwds):
            return fun(file, *args, **kwds)
        return result

    return {
        'utilities': {
            'analyzeVideo'     : wrap(analyzeVideo),
            'extractFrames'    : wrap(extractFrames),
            'getFile'          : wrap(getFile),
            'getVideoData'     : wrap(getVideoData),
            'cancelVideoJobs'  : wrap(cancelVideoJobs),
            'removeVideoData'  : wrap(removeVideoData),
            'transcodeVideo'   : wrap(transcodeVideo),
        },
        'getVideo'             : getVideo,
        'fetchVideoData'       : fetchVideoData,
        'getVideoFrame'        : getVideoFrame,
        'processVideo'         : processVideo,
        'deleteProcessedVideo' : deleteProcessedVideo,
        'convertVideo'         : convertVideo,
    }
