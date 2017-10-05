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

from cherrypy import HTTPRedirect

from bson.objectid import ObjectId

from girder import logger
from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import filtermodel, RestException, \
                            boundHandler, getCurrentUser

from girder.constants import AccessType, TokenScope
from girder.plugins.worker import utils as workerUtils

from ..constants import JobStatus


def addFileRoutes(file):
    routeHandlers = createRoutes(file)
    routeTable = (
        (
            'GET', (
                ((':id', 'video'),                    'getVideoMetadata'),
                ((':id', 'video', 'formats'),         'getVideoFormats'),
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


    def getFile(self, id):
        return (
            self.model('file')
                .load(id, user=getCurrentUser(), level=AccessType.READ))

    def getVideoData(self, id=None, file=None):
        if file is None:
            file = getFile(self, id)
        return file.get('video', {})


    def getVideoFormat(self, name=None, id=None, data=None):
        if data is None:
            data = getVideoData(self, id)

        if name is None:
            return data.get('sourceFormat', {})

        formatList = data.get('formats', [])
        for form in formatList:
            if form['name'] == name:
                return form

        raise RestException(
            'No such video format for file with id=%s' % id,
            code=404)


    def getCache(self,
            format=None, id=None, data=None, level=None, file=None,
            ensure=False):
        fileModel = self.model('file')
        folderModel = self.model('folder')
        collectionModel = self.model('collection')
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

        if data is None:
            file = getFile(self, id)
            data = getVideoData(self, file=file)

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

                    data['cacheId'] = str(folder['_id'])
                    fileModel.save(file)
                    return folderModel.load(
                        folder['_id'], level=level, user=cUser)

            return folder

        # video and format identified: ensure the given cache entry

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


    def cancelVideoJobs(self, id, format=None):
        file = getFile(self, id)
        vData = getVideoData(self, file=file)
        fileModel = self.model('file')
        jobModel = self.model('job', 'jobs')
        cUser = getCurrentUser()

        jobList = []
        formats = []
        if format is None:
            formats.push(vData.get('sourceFormat'))
            formats.extend(vData.get('formats', []))
        else:
            formats.push(getVideoFormat(self, name=format, data=vData))

        for form in formats:
            jobList.extend(form.pop('frameExtractionJobIds', []))
            jobList.push(form.pop('jobId', None))

        fileModel.save(file)

        def filterJob(jid):
            if jid:
                job = jobModel.load(jobId, level=AccessType.READ, user=cUser)
                if job:
                    jobModel.cancelJob(job)
                    return True
            return False

        jobList = list(filter(filterJob, jobList))

        return {
            'message': 'Video processing jobs canceled.',
            'fileId': id,
            'canceledJobs': jobList
        }


    def removeVideoData(self, id, format=None):
        file = getFile(self, id)
        vData = getVideoData(self, file=file)
        fileModel = self.model('file')
        folderModel = self.model('folder')
        cUser = getCurrentUser()

        formats = []
        if format is None:
            formats.push(vData.get('sourceFormat'))
            formats.extend(vData.get('formats', []))
        else:
            formats.push(getVideoFormat(self, name=format, data=vData))

        for form in formats:
            form.pop('fileId', None)
            form.pop('frames', None)

        folder = getCache(
            format=format, id=id, data=vData,
            level=AccessType.WRITE, file=file, ensure=False)

        if folder:
            folderModel.remove(folder)

        fileModel.save(file)

        return {
            'message': 'Processed video data deleted.',
            'fileId': id,
            'cacheFolderId': folder['_id'] if folder else None
        }


    @autoDescribeRoute(
        Description('Return video metadata if it exists.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format', 'Format of the video.', required=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
        .errorResponse('No such video format.', 404)
    )
    @access.public
    @boundHandler(file)
    def getVideoMetadata(self, id, params):
        return (
            getVideoFormat(self, name=params.get('format'), id=id)
                .get('metadata', {}))


    @autoDescribeRoute(
        Description('Return the list of alternate video formats.')
        .param('id', 'Id of the file.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def getVideoFormats(self, id, params):
        return getVideoData(self, id).get('formats', [])


    @autoDescribeRoute(
        Description('Return the named alternate video format.')
        .param('id', 'Id of the file.', paramType='path')
        .param('name', 'Name of the alternate video format.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
        .errorResponse('No such video format.', 404)
    )
    @access.public
    @boundHandler(file)
    def getVideoFormatByName(self, id, name, params):
        return getVideoFormat(self, name=name, id=id)


    @autoDescribeRoute(
        Description('Return a specific video frame.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format',
            'Name of the alternate video format.', required=False)
        .param('percent',
            'Portion of the video as a percentage (0-100).', required=False)
        .param('time',
            'Timestamp of the video in HH:MM:SS format.', required=False)
        .param('index',
            'Exact index of the desired frame.', required=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
        .errorResponse('No such video format.', 404)
        .errorResponse(
            'Missing metadata from format.', 400)
        .errorResponse(
            'Unrecognized timestamp format.', 400)
        .errorResponse(
            'Number of provided mutually-exclusive options != 1.', 400)
    )
    @access.public
    @boundHandler(file)
    def getVideoFrame(self, id, params):
        format = params.get('format')
        percent = params.get('percent')
        time = params.get('time')
        index = params.get('index')

        count = sum(x is not None for x in (percent, time, index))

        if count == 0 or count > 1:
            raise RestException(
                'Must provide no more than one of either '
                'percent, time, or index')

        formatData = getVideoFormat(self, name=format, id=id)

        frameCount = None
        duration = None
        if percent is not None or time is not None:
            frameCount = formatData.get('metadata', {}).get('frameCount')
            if frameCount is None:
                raise RestException('Missing metadata from format: frameCount')

            frameCount = int(frameCount)

            if time is not None:
                duration = formatData.get('metadata', {}).get('duration')
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

        # index guaranteed to not be None
        frameFileId = formatData.get('frames')[index]
        raise HTTPRedirect('api/v1/file/%s/download' % frameFileId)


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
        force = params['force']
        user, userToken = getCurrentUser(True)

        fileModel = self.model('file')
        folderModel = self.model('folder')
        tokenModel = self.model('token')
        jobModel = self.model('job', 'jobs')

        file = getFile(self, id)
        vData = getVideoData(self, file=file)
        form = getVideoFormat(self, id=id, data=vData)

        # TODO(opadron): check for vData['sourceFileId']
        jobId = form.get('jobId')

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
            cancelVideoJobs(self, id)
            removeVideoData(self, id)

        # begin construction of the actual job
        if not userToken:
            # It seems like we should be able to use a token without USER_AUTH
            # in its scope, but I'm not sure how.
            userToken = tokenModel.createToken(
                user, days=1, scope=TokenScope.USER_AUTH)

        cache = getCache(
            self, format=None, id=id, data=vData,
            level=AccessType.WRITE, file=file, ensure=True)

        analysis = folderModel.createFolder(
            parent=cache,
            name='analysis',
            description='Analysis cache for file %s' % str(id),
            parentType='folder',
            public=False,
            creator=user,
            reuseExisting=True)

        jobTitle = '[video] Initial Analysis'
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
            'progress_pipe': True,
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
                    'id': 'meta',
                    'type:': 'string',
                    'format': 'text',
                    'target': 'filepath',
                    'path': '/mnt/girder_worker/data/meta.json'
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
            '_stdout': workerUtils.girderOutputSpec(
                analysis,
                parentType='folder',
                token=userToken,
                name='stdout.txt',
                dataType='string',
                dataFormat='text',
                reference='videoPlugin:analysis:stdout'
            ),
            '_stderr': workerUtils.girderOutputSpec(
                analysis,
                parentType='folder',
                token=userToken,
                name='stderr.txt',
                dataType='string',
                dataFormat='text',
                reference='videoPlugin:analysis:stderr'
            ),
            'meta': workerUtils.girderOutputSpec(
                analysis,
                parentType='folder',
                token=userToken,
                name='meta.json',
                dataType='string',
                dataFormat='text',
                reference='videoPlugin:analysis:meta:%s' % str(id)
            ),
            ## 'source': workerUtils.girderOutputSpec(
            ##     analysis,
            ##     parentType='item',
            ##     token=userToken,
            ##     name='source.webm',
            ##     dataType='string',
            ##     dataFormat='text',
            ##     reference='videoPlugin'
            ## ),
        }

        job['kwargs']['jobInfo'] = workerUtils.jobInfoSpec(
            job=job,
            token=jobToken,
            logPrint=True)

        job['meta'] = job.get('meta', {})
        job['meta']['video_plugin'] = { 'fileId': id }

        job = jobModel.save(job)
        jobModel.scheduleJob(job)

        form['jobId'] = str(job['_id'])
        fileModel.save(file)

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
        .param('id', 'Id of the file.', paramType='path')
        .errorResponse()
        .errorResponse('Write access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteProcessedVideo(self, id, params):
        cancelResponse = cancelVideoJobs(self, id)
        removeResponse = removeVideoData(self, id)

        return {
            'message': 'Video processing jobs canceled.',
            'fileId': id,
            'canceledJobs': jobList
        }
        return {
            'message': 'Processed video data deleted.',
            'fileId': id,
            'cacheFolderId': folder['_id'] if folder else None
        }

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
        .param('name', 'Name for the new format.', paramType='path')
        .param('dimensions', 'Dimensions of the video.', required=False)
        .param('frameRate', 'Fram rate of the video.', required=False)
        .param('videoCodec', 'Video codec for the video.', required=False)
        .param('audioCodec', 'Audio codec for the video.', required=False)
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
    def addVideoFormat(params):
        pass


    @autoDescribeRoute(
        Description('Add a new format for the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('name', 'Name for the new format.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteVideoFormat(params):
        pass


    return {
        'getVideoMetadata'     : getVideoMetadata,
        'getVideoFormats'      : getVideoFormats,
        'getVideoFormatByName' : getVideoFormatByName,
        'getVideoFrame'        : getVideoFrame,
        'processVideo'         : processVideo,
        'addVideoFormat'       : addVideoFormat,
        'deleteVideoFormat'    : deleteVideoFormat,
        'deleteProcessedVideo' : deleteProcessedVideo,
    }

