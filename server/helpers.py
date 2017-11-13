"""
Collection of useful routines for handling most of the data management tasks for
the video plugin.
"""

import json
import os.path

from functools import wraps

from .utils import objectIdOrNone
from .constants import JobStatus, VideoEnum

from girder.api.rest import RestException, getCurrentUser

from girder.constants import AccessType, TokenScope
from girder.plugins.worker.utils import \
        jobInfoSpec, girderInputSpec, girderOutputSpec
from girder.models.model_base import ValidationException

def resolveFileId(self, fileId, formatId=None, formatName=None):
    videoModel = self.model('video', 'video')

    fileId = objectIdOrNone(fileId)

    if not formatId and formatName:
        formatId = getVideoFormatByName(self, formatName)['_id']
    else:
        formatId = objectIdOrNone(formatId)

    isFile = { 'type': VideoEnum.FILE }
    isFrame = { 'type': VideoEnum.FRAME }
    isData = { '$or': [isFile, isFrame] }

    matchesSource = { 'sourceFileId': fileId }
    notMatchesSource = { 'sourceFileId': { '$ne': fileId } }
    matchesFile = { 'fileId': fileId }
    matchesFormat = { 'formatId': formatId }

    # first, check to see if fileId refers to a file created by the plugin
    data = videoModel.findOne({'$and': [
        isData,
        notMatchesSource,
        matchesFile
    ]})

    if data:
        # if it is, and it's a FRAME, bail, completely (users should not be
        # handing the plugin the fileIds of its own generated frames).
        if data['type'] == VideoEnum.FRAME:
            return None, None, None

        # if it is a FILE, then ignore the formatId provided in the call
        return data['sourceFileId'], fileId, data['formatId']

    # if we get this far, then the provided fileId can only be a sourceFileId
    data = videoModel.findOne({'$and': [
        isFile,
        matchesSource,
        matchesFormat
    ]})

    return fileId, (data or {}).get('fileId'), formatId

def getFile(self, id, user=None, level=None):
    if user is None:
        user = getCurrentUser()

    if level is None:
        level = AccessType.READ

    result = self.model('file').load(id, user=user, level=level)
    return result

def getVideoData(self, fileId=None, formatId=None, user=None, ensure=False):
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

    _id = objectIdOrNone(id)

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

    _id = objectIdOrNone(id)

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
            if file is not None:
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
                    'message': 'Analysis job already created.'
                }
            }

            result.update(job)
            return result

    # if we are *re*running an analysis job (force=True), remove all data
    # for this video that were created by the last analysis job, and all data
    # create by jobs that depend on the results of the analysis job
    if force:
        cancelVideoJobs(
            self, fileId, formatId=formatId, user=user)
        removeVideoData(
            self, fileId, formatId=formatId, user=user)

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
        'input': girderInputSpec(
            targetFile,
            resourceType='file',
            token=userToken,
            name='input' + itemExt,
            dataType='string',
            dataFormat='text'
        )
    }

    job['kwargs']['outputs'] = {
        'meta': girderOutputSpec(
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
                    dataType=VideoEnum.ANALYSIS
                ))))
            ))))
        )
    }

    job['kwargs']['jobInfo'] = jobInfoSpec(
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
            '_id': objectIdOrNone(formatId)
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
                    'message': 'Transcoding job already created.'
                }
            }

            result.update(job)
            return result

    # if we are *re*running a transcoding job (force=True), remove all data
    # for this video that were created by the last transcoding job, and any
    # frames extracted from the old results
    if force:
        cancelVideoJobs(self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.TRANSCODING | VideoEnum.FRAME_EXTRACTION,
            cascade=False)

        removeVideoData(self, fileId, formatId=formatId, user=user,
            mask=VideoEnum.FILE | VideoEnum.FRAME,
            cascade=False)

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
        'input': girderInputSpec(
            file,
            resourceType='file',
            token=userToken,
            name='input' + itemExt,
            dataType='string',
            dataFormat='text'
        )
    }

    job['kwargs']['outputs'] = {
        'output': girderOutputSpec(
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
                    dataType=VideoEnum.TRANSCODING
                ))))
            ))))
        )
    }

    job['kwargs']['jobInfo'] = jobInfoSpec(
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

from .utils import debug, loud

@loud
@debug
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
                    'message': 'Extraction job already created.'
                }
            }

            result.update(job)
            return result

    # if we are *re*running a frame extraction job (force=True), remove all
    # frames for this video that were created by the last extraction job
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
        'input': girderInputSpec(
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
            girderOutputSpec(
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
                        dataType=VideoEnum.FRAME_EXTRACTION,
                        index=index
                    ))))
                ))))
            )
        )
        for index in range(numFrames)
    )

    job['kwargs']['jobInfo'] = jobInfoSpec(
        job=job,
        token=jobToken,
        logPrint=True)

    job = jobModel.save(job)
    jobModel.scheduleJob(job)

    vJob = videoModel.createVideoJob(
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

def getVideoFormats(self, id=None):
    videoModel = self.model('video', 'video')

    query = { 'type': VideoEnum.FORMAT }

    if id:
        formatIds = set()
        for vData in videoModel.find({'sourceFileId': id,
                                      'type': VideoEnum.FILE}):
            if vData['formatId']:
                formatIds.add(vData['formatId'])

        correctId = [{'_id': _id} for _id in formatIds]
        correctId = (
            { 'type': 0xFFFF }  # should not match anything
            if len(correctId) == 0 else

            correctId[0]
            if len(correctId) == 1 else

            { '$or': correctId }
        )
        query = { '$and': [query, correctId] }

    return list(videoModel.find(query))

def getVideoFormatByName(self, name, id=None):
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


from types import ModuleType
helpers = ModuleType('helpers')
def initHelpers(fileApi):
    from functools import wraps
    from inspect import getmodule

    if helpers.__doc__:
        return

    def wrap(fun):
        @wraps(fun)
        def result(*args, **kwargs):
            return fun(fileApi, *args, **kwargs)
        return result

    module = getmodule(initHelpers)

    helpers.__dict__.update(dict(
        (key, wrap(func))
        for key, func in module.__dict__.items()
        if (func is not initHelpers and
            getmodule(func) is module and
            not isinstance(func, type) and
            hasattr(func, '__call__'))
    ))
    helpers.__doc__ = 'auto-generated module'
