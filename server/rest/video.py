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

from functools import wraps

from cherrypy import HTTPRedirect

from bson.objectid import ObjectId

from girder import logger
from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import filtermodel, RestException, \
                            boundHandler, getCurrentUser

from girder.constants import AccessType, TokenScope
from girder.plugins.worker import utils as workerUtils
from girder.models.model_base import ValidationException

from ..constants import JobStatus


def addFileRoutes(file):
    routeHandlers = createRoutes(file)
    routeTable = (
        (
            'GET', (
                ((':id', 'video'),                    'getVideo'),
                ((':id', 'video', 'format'),          'getVideoFormats'),
                ((':id', 'video', 'sourceFormat'),    'getSourceVideoFormat'),
                ((':id', 'video', 'format', ':name'), 'getVideoFormatByName'),
                ((':id', 'video', 'frame'),           'getVideoFrame'),
            )
        ),
        (
            'POST', (
                ((':id', 'video'),                    'processVideo'),
                ((':id', 'video', 'format'),          'addVideoFormat'),
            )
        ),
        (
            'DELETE', (
                ((':id', 'video', 'format', ':name'), 'deleteVideoFormat'),
                ((':id', 'video'),                    'deleteProcessedVideo'),
            )
        ),
    )

    for method, routes in routeTable:
        for route, key in routes:
            file.route(method, route, routeHandlers[key])

    return routeHandlers


def createRoutes(file):
    def ensureVideoUser(self):
        userModel = self.model('user')

        user = userModel.findOne({ 'login': 'internalvideouser' })
        if not user:
            user = userModel.createUser(
                login='internalvideouser',
                password='unused',
                email='internal@video.user',
                firstName='Internal',
                lastName='Video User',
                admin=True,
                public=False
            )

            user['status'] = 'disabled'

            user = userModel.save(user)

        return user


    def getFile(self, id, user=None, level=None):
        if user is None:
            user = getCurrentUser()

        if level is None:
            level = AccessType.READ

        result = self.model('file').load(id, user=user, level=level)
        return result


    def getVideoData(
            self, format=None, id=None, file=None,
            user=None, level=None, ensure=False):
        videodataModel = self.model('videodata', 'video')

        if user is None:
            user = getCurrentUser()

        if level is None:
            level = AccessType.WRITE

        if file is None:
            file = getFile(self, id, user=user)

        data = None
        if format is None:
            data = videodataModel.findOne({
                'fileId': file['_id'],
                'sourceFileId': file['_id']
            })

            if not data and ensure:
                data = videodataModel.createVideodata(fileId=file['_id'])
        else:
            data = videodataModel.findOne({
                'sourceFileId': file['_id'],
                'formatName': format
            })

            if not data and ensure:
                data = videodataModel.createVideodata(
                    sourceFileId=file['_id'], formatName=format)

        if data:
            data = videodataModel.load(data['_id'], user=user, level=level)

        return data


    def getCache(self,
            format=None, id=None, data=None, user=None,
            level=None, file=None, ensure=False):
        fileModel = self.model('file')
        folderModel = self.model('folder')
        collectionModel = self.model('collection')
        videodataModel = self.model('videodata', 'video')

        cUser = user
        if not cUser:
            cUser = getCurrentUser()

        vUser = ensureVideoUser(self)

        if id is None:  # no video identified: ensure the main cache collection
            cacheRoot = collectionModel.findOne({ 'name': '_videoCache' })
            if not cacheRoot and ensure:
                cacheRoot = collectionModel.createCollection(
                    name='_videoCache',
                    description='Video Cache',
                    public=False,
                    creator=vUser)

            if cacheRoot:
                cacheRoot = collectionModel.load(
                    cacheRoot['_id'], level=level, user=vUser)

            return cacheRoot

        # video identified: ensure the cache for the given format

        if file is None:
            file = getFile(self, id, user=cUser)

        if data is None:
            data = getVideoData(
                self, file=file, user=cUser,
                level=AccessType.WRITE, ensure=True)

        if format is None:  # no format identified: ensure the source cache
            cacheRoot = getCache(
                    self, id=None, level=AccessType.WRITE, ensure=ensure)

            folder = None
            if cacheRoot:
                folder = folderModel.findOne({
                    'parentId': cacheRoot['_id'], 'name': str(id) })

                if not folder and ensure:
                    folder = folderModel.createFolder(
                        parent=cacheRoot,
                        name=str(id),
                        description='Video cache for file %s' % str(id),
                        parentType='collection',
                        public=False,
                        creator=cUser,
                        reuseExisting=True)

                    acl = {
                        'users': [{
                            'id': cUser['_id'],
                            'level': AccessType.ADMIN
                        }]
                    }

                    folder = folderModel.setAccessList(
                            folder, acl, user=vUser, save=True, force=True)

                    data['cacheId'] = folder['_id']
                    videodataModel.save(data)

                    folder = folderModel.load(
                        folder['_id'], level=level, user=cUser)

            return folder

        # video and format identified: ensure the given cache entry

        formatData = getVideoData(
            self, format=format, file=file, user=cUser,
            level=AccessType.WRITE, ensure=True)

        cache = getCache(
            self, format=None, id=id, data=data,
            level=AccessType.WRITE, file=file, ensure=ensure)

        formatFolder = None
        if cache:
            if ensure:
                formatsFolder = folderModel.load(
                    folderModel.createFolder(
                        parent=cache,
                        name='formats',
                        description='Format cache for file %s' % str(id),
                        parentType='folder',
                        public=False,
                        creator=cUser,
                        reuseExisting=True)['_id'],
                    level=AccessType.WRITE,
                    user=cUser)

                formatFolder = folderModel.load(
                    folderModel.createFolder(
                        parent=formatsFolder,
                        name=format,
                        description=(
                            'Cache for file %s(%s format)' % (str(id), format)),
                        parentType='folder',
                        public=False,
                        creator=cUser,
                        reuseExisting=True)['_id'],
                    level=level,
                    user=cUser)

                formatData['cacheId'] = formatFolder['_id']
                videodataModel.save(formatData)

            else:
                formatsFolder = folderModel.findOne({
                    'parentId': cache['_id'], 'name': 'formats' })

                if formatsFolder:
                    formatFolder = folderModel.findOne({
                        'parentId': formatsFolder['_id'], 'name': format })

                    if formatFolder:
                        formatFolder = folderModel.load(
                            formatFolder['_id'], level=level, user=cUser)

        return formatFolder


    def cancelVideoJobs(self, id, format=None, user=None):
        videojobModel = self.model('videojob', 'video')
        jobModel = self.model('job', 'jobs')

        cUser = user

        if not cUser:
            cUser = getCurrentUser()

        canceledJobs = set()
        query = (
            { 'sourceFileId': ObjectId(id), 'formatName': format }
            if format else
            { '$or': [{ 'sourceFileId': ObjectId(id) },
                      { 'fileId'      : ObjectId(id) }] }
        )

        for vJob in videojobModel.find(query):
            vJob = videojobModel.load(
                vJob['_id'], level=AccessType.WRITE, user=cUser)

            job = jobModel.load(
                vJob['jobId'], level=AccessType.WRITE, user=cUser)

            try:
                jobModel.cancelJob(job)
                canceledJobs.add(str(job['_id']))
            except ValidationException:
                pass

            videojobModel.remove(vJob)

        return {
            'message': 'Video processing jobs canceled.',
            'fileId': id,
            'canceledJobs': list(canceledJobs)
        }


    def removeVideoData(self, id, format=None, user=None):
        folderModel = self.model('folder')
        videodataModel = self.model('videodata', 'video')
        videoframeModel = self.model('videoframe', 'video')

        cUser = user

        if not cUser:
            cUser = getCurrentUser()

        query = (
            { 'sourceFileId': ObjectId(id), 'formatName': format }
            if format else
            { '$or': [{ 'sourceFileId': ObjectId(id) },
                      { 'fileId'      : ObjectId(id) }] }
        )

        for vFrame in videoframeModel.find(query):
            vFrame = videoframeModel.load(
                vFrame['_id'], level=AccessType.WRITE, user=cUser)
            videoframeModel.remove(vFrame)

        vData = getVideoData(
            self, format=format, id=id, user=cUser,
            level=AccessType.WRITE, ensure=False)

        folder = getCache(
            self, format=format, id=id, user=cUser,
            data=vData, level=AccessType.WRITE, ensure=False)

        if folder:
            folderModel.remove(folder)

            vData['cacheId'] = None
            videodataModel.save(vData)

        for vData in videodataModel.find(query):
            vData = videodataModel.load(
                    vData['_id'], level=AccessType.WRITE, user=cUser)
            if vData:
                videodataModel.remove(vData)

        return {
            'message': 'Processed video data deleted.',
            'fileId': id,
            'cacheFolderId': folder['_id'] if folder else None
        }


    def analyzeVideo(self, id, force=False, format=None, user=None):
        userToken = None

        if user is None:
            user, userToken = getCurrentUser(True)

        fileModel = self.model('file')
        folderModel = self.model('folder')
        tokenModel = self.model('token')
        jobModel = self.model('job', 'jobs')
        videodataModel = self.model('videodata', 'video')
        videojobModel = self.model('videojob', 'video')

        file = getFile(self, id)
        vData = getVideoData(
            self, file=file, format=format, user=user, ensure=True)

        vJob = videojobModel.findOne({
            'type': 'analysis', 'fileId': vData['fileId'],
            'sourceFileId': vData['sourceFileId'] })

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
        if force and not format:
            cancelVideoJobs(self, id, user=user)
            removeVideoData(self, id, user=user)

        # begin construction of the actual job
        if not userToken:
            # It seems like we should be able to use a token without USER_AUTH
            # in its scope, but I'm not sure how.
            userToken = tokenModel.createToken(
                user, days=1, scope=TokenScope.USER_AUTH)

        cache = getCache(
            self, format=format, id=id, data=vData, user=user,
            level=AccessType.WRITE, file=file, ensure=True)

        targetFile = getFile(self, vData['fileId'], user=user)
        analysis = folderModel.createFolder(
            parent=cache,
            name='analysis',
            description='Analysis cache for file %s' % str(targetFile['_id']),
            parentType='folder',
            public=False,
            creator=user,
            reuseExisting=True)

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
                analysis,
                parentType='folder',
                token=userToken,
                name='meta.json',
                dataType='string',
                dataFormat='text',
                reference=(
                    'videoPlugin:analysis:%s:%s' % (str(id), format)
                    if format else
                    'videoPlugin:analysis:%s' % str(id)
                )
            ),
        }

        job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
            job=job,
            token=jobToken,
            logPrint=True)

        job['meta'] = job.get('meta', {})
        job['meta']['video_plugin'] = { 'fileId': id }

        job = jobModel.save(job)
        jobModel.scheduleJob(job)

        videojobModel.createVideojob(
            fileId=vData['fileId'],
            sourceFileId=vData['sourceFileId'],
            formatName=format,
            type='analysis',
            jobId=job['_id'])

        videodataModel.save(vData)

        result = {
            'video': {
                'jobCreated': True,
                'message': 'Processing job created.'
            }
        }

        result.update(job)
        return result


    def extractFrames(self, id, numFrames, format=None, user=None):
        userToken = None

        if user is None:
            user, userToken = getCurrentUser(True)

        fileModel = self.model('file')
        folderModel = self.model('folder')
        tokenModel = self.model('token')
        jobModel = self.model('job', 'jobs')
        videojobModel = self.model('videojob', 'video')

        file = getFile(self, id, user=user)
        vData = getVideoData(
            self, format=format, file=file, user=user, level=AccessType.READ)

        inputFile = file
        if format:
            inputFile = getFile(self, vData['fileId'], user=user)

        if not userToken:
            userToken = tokenModel.createToken(
                user, days=1, scope=TokenScope.USER_AUTH)

        cache = getCache(
            self, format=format, id=id, data=vData, user=user,
            level=AccessType.WRITE, file=file, ensure=True)

        frames = folderModel.createFolder(
            parent=cache,
            name='frames',
            description='Frame cache for file %s' % str(inputFile['_id']),
            parentType='folder',
            public=False,
            creator=user,
            reuseExisting=True)

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
                    frames,
                    parentType='folder',
                    token=userToken,
                    name='%d.png' % index,
                    dataType='string',
                    dataFormat='text',
                    reference=(
                        'videoPlugin:frame:%s:%s:%d' % (format, str(id), index)
                        if format else
                        'videoPlugin:frame:%s:%d' % (str(id), index)
                    )
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

        vJob = None
        if format is not None:
            vJob = videojobModel.createVideojob(
                sourceFileId=id, type='frameExtraction',
                formatName=format, jobId=job['_id'])
        else:
            vJob = videojobModel.createVideojob(
                fileId=id, type='frameExtraction', jobId=job['_id'])

        result = {
            'video': {
                'jobCreated': True,
                'message': 'Frame extraction job created.'
            }
        }

        result.update(job)
        return result

    def _getVideoFormatByName(self, id, name, params):
        videodataModel = self.model('videodata', 'video')
        user = getCurrentUser()

        result = None
        if name:
            result = videodataModel.findOne({
                'sourceFileId': ObjectId(id), 'formatName': name })
        else:
            result = videodataModel.findOne({ 'fileId': ObjectId(id) })

        if result:
            result = videodataModel.load(
                result['_id'], level=AccessType.READ, user=user)

        if result:
            result = {
                'metadata': result['metadata'],
                'name': (result['formatName'] if name else '[SOURCE]')
            }

            return result

        raise RestException('No such video format', 404)


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
        fileModel = self.model('file')
        format = params.get('format')

        sourceFile = getFile(self, id)

        vData = getVideoData(
            self, format=format, file=sourceFile,
            ensure=False, level=AccessType.READ)

        if not vData:
            raise RestException('No such video format', 404)

        targetFile = getFile(self, vData['fileId'])

        return self.download(id=str(targetFile['_id']), params=dict(
            item for item in params.items() if item[1] is not None
        ))


    @autoDescribeRoute(
        Description('Return the list of alternate video formats.')
        .param('id', 'Id of the file.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def getVideoFormats(self, id, params):
        videodataModel = self.model('videodata', 'video')
        user = getCurrentUser()

        result = [
            {
                'name': x['formatName'],
                'metadata': x['metadata']
            }
            for x in (
                videodataModel.load(vData['_id'], level=AccessType.READ, user=user)
                for vData in videodataModel.find(
                    { 'sourceFileId': ObjectId(id) })
            )
            if str(x['fileId']) != str(x['sourceFileId'])
        ]

        return result


    @autoDescribeRoute(
        Description('Return the source video format.')
        .param('id', 'Id of the file.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
        .errorResponse('No such video format.', 404)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def getSourceVideoFormat(self, id, params):
        return _getVideoFormatByName(self, id, None, params)


    @autoDescribeRoute(
        Description('Return the named alternate video format.')
        .param('id', 'Id of the file.', paramType='path')
        .param('name', 'Name of the alternate video format.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
        .errorResponse('No such video format.', 404)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def getVideoFormatByName(self, id, name, params):
        return _getVideoFormatByName(self, id, name, params)


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
        videoframeModel = self.model('videoframe', 'video')

        count = sum(x is not None for x in (percent, time, index))

        if count == 0 or count > 1:
            raise RestException(
                'Must provide no more than one of either '
                'percent, time, or index')

        vData = getVideoData(self, format=format, id=id, level=AccessType.READ)

        frameCount = None
        duration = None
        if percent is not None or time is not None:
            frameCount = (
                vData
                    .get('metadata', {})
                    .get('video', {})
                    .get('frameCount', 0))

            if not frameCount:
                raise RestException('Missing metadata from format: frameCount')

            frameCount = int(frameCount)

            if time is not None:
                duration = vData.get('metadata', {}).get('duration')
                if duration is None:
                    raise RestException(
                        'Missing metadata from format: duration')

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

        query = (
            {
                '$and': [
                    { 'sourceFileId': ObjectId(id), 'formatName': format }
                    if format else
                    { '$or': [{ 'sourceFileId': ObjectId(id) },
                              { 'fileId'      : ObjectId(id) }] },

                    { 'index': index }
                ]
            }
        )

        frame = videoframeModel.findOne(query)
        if frame:
            file = fileModel.findOne({ 'itemId': frame['itemId'] })
            if file:
                return self.download(id=str(file['_id']), params=dict(
                    item for item in params.items() if item[1] is not None
                ))

        raise RestException('Frame not found.', 404)


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
        Description('Add a new format for the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('name', 'Name for the new format.', required=True)
        .param('force', 'Force the creation of a new job.', required=False,
            dataType='boolean', default=False)
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
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def addVideoFormat(self, id, name, params):
        force = params['force']
        user, userToken = getCurrentUser(True)

        fileModel = self.model('file')
        folderModel = self.model('folder')
        tokenModel = self.model('token')
        jobModel = self.model('job', 'jobs')
        videodataModel = self.model('videodata', 'video')
        videojobModel = self.model('videojob', 'video')

        file = getFile(self, id)
        originalFileName, _ = os.path.splitext(file['name'])

        metadata = getVideoData(self, file=file, ensure=True).get('metadata')

        video = metadata.get('video', {})
        audio = metadata.get('audio', {})

        videoCodec = params.get('videoCodec')
        audioCodec = params.get('audioCodec')

        dimensions = params.get('dimensions')
        if not dimensions:
            width = video.get('width')
            height = video.get('height', 0)

            if width and height:
                dimensions = '%dx%d' % (width, height)

        audioSampleRate = params.get('audioSampleRate', audio.get('sampleRate'))
        audioBitRate = params.get('audioBitRate', audio.get('bitRate'))
        videoBitRate = params.get('videoBitRate', video.get('bitRate'))

        vData = getVideoData(self, file=file, format=name, ensure=True)

        vJob = videojobModel.findOne({
            'type': 'transcoding',
            'sourceFileId': ObjectId(id),
            'formatName': name })

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
            cancelVideoJobs(self, id, format=name, user=user)
            removeVideoData(self, id, format=name, user=user)

        # begin construction of the actual job
        if not userToken:
            userToken = tokenModel.createToken(
                user, days=1, scope=TokenScope.USER_AUTH)

        cache = getCache(
            self, format=name, id=id, data=vData,
            level=AccessType.WRITE, file=file, ensure=True)

        transcodingFolder = folderModel.createFolder(
            parent=cache,
            name='transcoding',
            description='Transcoding cache for file %s, format %s' % (
                str(id), name),
            parentType='folder',
            public=False,
            creator=user,
            reuseExisting=True)

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
                transcodingFolder,
                parentType='folder',
                token=userToken,
                name='%s_%s.%s' % (originalFileName, name, fileExtension),
                dataType='string',
                dataFormat='text',
                reference='videoPlugin:transcoding:%s:%s' % (str(id), name)
            )
        }

        job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
            job=job,
            token=jobToken,
            logPrint=True)

        job['meta'] = job.get('meta', {})
        job['meta']['video_plugin'] = { 'fileId': id }

        job = jobModel.save(job)
        jobModel.scheduleJob(job)

        videojobModel.createVideojob(
            sourceFileId=id, type='transcoding',
            formatName=name, jobId=job['_id'])

        videodataModel.save(vData)

        result = {
            'video': {
                'jobCreated': True,
                'message': 'Processing job created.'
            }
        }

        result.update(job)
        return result


    @autoDescribeRoute(
        Description('Remove a format from the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('name', 'Name for the new format.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteVideoFormat(self, id, name, params):
        cancelResponse = cancelVideoJobs(self, id, format=name)
        removeResponse = removeVideoData(self, id, format=name)

        cancelResponse.pop('message', None)
        removeResponse.pop('message', None)
        removeResponse.pop('fileId', None)

        response = {}
        response.update(cancelResponse)
        response.update(removeResponse)
        response['message'] = 'Processed video format data deleted.'

        return response

    def wrap(fun):
        @wraps(fun)
        def result(*args, **kwds):
            return fun(file, *args, **kwds)
        return result

    return {
        'utilities': {
            'analyzeVideo'     : wrap(analyzeVideo),
            'ensureVideoUser'  : wrap(ensureVideoUser),
            'extractFrames'    : wrap(extractFrames),
            'getFile'          : wrap(getFile),
            'getVideoData'     : wrap(getVideoData),
            'getCache'         : wrap(getCache),
            'cancelVideoJobs'  : wrap(cancelVideoJobs),
            'removeVideoData'  : wrap(removeVideoData),
        },
        'getSourceVideoFormat' : getSourceVideoFormat,
        'getVideo'             : getVideo,
        'getVideoFormats'      : getVideoFormats,
        'getVideoFormatByName' : getVideoFormatByName,
        'getVideoFrame'        : getVideoFrame,
        'processVideo'         : processVideo,
        'addVideoFormat'       : addVideoFormat,
        'deleteVideoFormat'    : deleteVideoFormat,
        'deleteProcessedVideo' : deleteProcessedVideo,
    }

