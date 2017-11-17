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

from ..constants import VideoEnum

from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import RestException, Resource, boundHandler

from girder.constants import TokenScope

from .. import helpers


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
        return helpers.getVideoFormats(self)

    @autoDescribeRoute(
        Description('Look up a defined video format by name.')
        .param('name', 'Name of the defined video fromat', paramType='path',
               required=True)
        .errorResponse()
        .errorResponse('No such video format.', 404)
    )
    @access.public(scope=TokenScope.DATA_READ)
    def getFormatByName(self, name, params):
        return helpers.getVideoFormatByName(self, name)

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
                ((':id', 'video'),                  'getVideo'),
                ((':id', 'video', 'stream'),        'getVideo'),
                ((':id', 'video', 'metadata'),      'getVideoData'),
                ((':id', 'video', 'frame'),         'getVideoFrame'),
            )
        ),
        (
            'PUT', (
                ((':id', 'video'),                  'analyzeVideo'),
                ((':id', 'video', 'analysis'),      'analyzeVideo'),
                ((':id', 'video', 'transcoding'),   'transcodeVideo'),
                ((':id', 'video', 'extraction'),    'extractFrames'),
            )
        ),
        (
            'DELETE', (
                ((':id', 'video'),                  'deleteVideoAnalysis'),
                ((':id', 'video', 'analysis'),      'deleteVideoAnalysis'),
                ((':id', 'video', 'transcoding'),   'deleteVideoTranscoding'),
                ((':id', 'video', 'extraction'),    'deleteVideoFrames'),
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
        sourceId, targetId, formatId = (
                helpers.resolveFileId(self, id, formatName=format))

        if not targetId:
            msg = 'No source video found'
            if format:
                msg = (
                    "Source video has not been "
                    "transcoded to format: '{}'".format(format))

            raise RestException(msg, 404)

        return self.download(id=str(targetId), params=dict(
            item for item in params.items() if item[1] is not None
        ))


    @autoDescribeRoute(
        Description('Return the data for the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format',
               'Name of the format, or leave blank for the source video.',
               required=False)
        .errorResponse()
        .errorResponse(
            'Source video has not been transcoded into Format.', 404)
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.cookie
    @access.public(scope=TokenScope.DATA_READ)
    @boundHandler(file)
    def getVideoData(self, id, params):
        format = params.get('format')
        sourceId, targetId, formatId = (
                helpers.resolveFileId(self, id, formatName=format))
        file = helpers.getFile(self, sourceId)

        result = helpers.getVideoData(self, fileId=sourceId, formatId=formatId)
        if not result:
            msg = 'No source video found'
            if format:
                msg = (
                    "Source video has not been "
                    "transcoded to format: '{}'".format(format))

            raise RestException(msg, 404)

        result.pop('_id', None)
        result.pop('fileId', None)
        result.pop('sourceFileId', None)
        result.pop('formatId', None)
        result.pop('type', None)

        if not formatId:
            result['availableFormats'] = sorted([
                form['name'] for form in
                helpers.getVideoFormats(self, sourceId)])

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

        sourceId, targetId, formatId = (
                helpers.resolveFileId(self, id, formatName=format))

        count = sum(x is not None for x in (percent, time, index))

        if count == 0 or count > 1:
            raise RestException(
                'Must provide no more than one of either '
                'percent, time, or index')

        vData = helpers.getVideoData(
            self, fileId=sourceId, formatId=formatId)

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
            'sourceFileId': sourceId,
            'formatId': formatId,
            'index': index
        }

        frame = videoModel.findOne(query)
        if frame:
            return self.download(id=str(frame['fileId']), params=dict(
                item for item in params.items() if item[1] is not None
            ))

        raise RestException('Frame not found.', 404)


    @autoDescribeRoute(
        Description('Create a girder-worker job to analyze the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('force', 'Force the creation of a new job.', required=False,
            dataType='boolean', default=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def analyzeVideo(self, id, params):
        sourceId, targetId, formatId = helpers.resolveFileId(self, id)
        return helpers.analyzeVideo(
            self, sourceId, formatId=formatId,
            force=params.get('force', False))


    @autoDescribeRoute(
        Description('Delete the analysis results from the given video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format',
               'Restrict the data removal to the given format. '
               'If provided, only the copy that was transcoded into the '
               'given format and their extracted frames are removed.',
               required=False)
        .notes(
            '&middot; Also deletes all extracted frames, all transcoded '
            'copies, their analysis results, and their extracted frames.'
            '<br>'
            '&middot; This endpoint will <strong>never</strong> delete the '
            'source video file. To delete all video-related data, as well as '
            'the source file, use <a href="#!/file/file_deleteFile">'
            'DELETE /file/:id</a>.'
        )
        .errorResponse()
        .errorResponse('Write access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteVideoAnalysis(self, id, params):
        sourceId, targetId, formatId = helpers.resolveFileId(self, id)

        cancelResponse = helpers.cancelVideoJobs(
            self, sourceId, formatId=formatId)
        removeResponse = helpers.removeVideoData(
            self, sourceId, formatId=formatId)

        cancelResponse.pop('message', None)
        removeResponse.pop('message', None)
        removeResponse.pop('fileId', None)

        response = {}
        response.update(cancelResponse)
        response.update(removeResponse)
        response['message'] = 'Processed video data deleted.'

        return response


    @autoDescribeRoute(
        Description('Delete the transcoded version of the given video.')
        .notes('Also deletes all frames extracted from the transcoded version.')
        .param('id', 'Id of the source file.', paramType='path')
        .param('format',
               'The format for which the transcoded version should be deleted.',
               required=True)
        .notes('This endpoint will never delete the source video file. '
               'To delete all video-related data, as well as the source file, '
                '<a href="#!/file/file_deleteFile">DELETE /file/:id</a>.')
        .errorResponse()
        .errorResponse('Write access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteVideoTranscoding(self, id, params):
        sourceId, targetId, formatId = helpers.resolveFileId(
            self, id, formatName=params.get('format'))

        cancelResponse = helpers.cancelVideoJobs(
            self, sourceId, formatId=formatId,
            mask=VideoEnum.TRANSCODING | VideoEnum.FRAME_EXTRACTION,
            cascade=False)

        removeResponse = helpers.removeVideoData(
            self, sourceId, formatId=formatId,
            mask=VideoEnum.FILE | VideoEnum.FRAME,
            cascade=False)

        cancelResponse.pop('message', None)
        removeResponse.pop('message', None)
        removeResponse.pop('fileId', None)

        response = {}
        response.update(cancelResponse)
        response.update(removeResponse)
        response['message'] = 'Processed video data deleted.'

        return response


    @autoDescribeRoute(
        Description('Delete the extracted frames from the given video.')
        .param('id', 'Id of the source file.', paramType='path')
        .param('format',
               'The format for which the extracted frames should be deleted, '
               'or if blank, remove the frames extracted from the source '
               'video.',
               required=False)
        .notes('This endpoint will never delete the source video file. '
               'To delete all video-related data, as well as the source file, '
                '<a href="#!/file/file_deleteFile">DELETE /file/:id</a>.')
        .errorResponse()
        .errorResponse('Write access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def deleteVideoFrames(self, id, params):
        sourceId, targetId, formatId = helpers.resolveFileId(
            self, id, formatName=params.get('format'))

        cancelResponse = helpers.cancelVideoJobs(
            self, sourceId, formatId=formatId,
            mask=VideoEnum.FRAME_EXTRACTION,
            cascade=False)

        removeResponse = helpers.removeVideoData(
            self, sourceId, formatId=formatId,
            mask=VideoEnum.FRAME,
            cascade=False)

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
        .param('force', 'Force the creation of a new transcoding job.',
            required=False, dataType='boolean', default=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def transcodeVideo(self, id, format, params):
        sourceId, targetId, formatId = (
                helpers.resolveFileId(self, id, formatName=format))

        return helpers.transcodeVideo(
            self, sourceId, formatId=formatId,
            force=params.get('force', False))


    @autoDescribeRoute(
        Description('Extract the frames from a video.')
        .param('id', 'Id of the file.', paramType='path')
        .param('format',
               'Name of the format, or leave blank for the source video.',
               required=False)
        .param('force', 'Force the creation of a new job.', required=False,
            dataType='boolean', default=False)
        .errorResponse()
        .errorResponse('Read access was denied on the file.', 403)
    )
    @access.public
    @boundHandler(file)
    def extractFrames(self, id, params):
        sourceId, targetId, formatId = helpers.resolveFileId(
                self, id, formatName=params.get('format'))

        vData = helpers.getVideoData(self, fileId=sourceId, formatId=formatId)
        if not vData:
            raise RestException('Video data not found', 404)

        frameCount = vData.get('videoFrameCount', 0)
        if not frameCount:
            raise RestException(
                'Missing data from video: videoFrameCount')

        return helpers.extractFrames(
            self, sourceId, frameCount,
            formatId=formatId, force=params.get('force', False))

    return {
        'getVideo'              : getVideo,
        'getVideoData'          : getVideoData,
        'getVideoFrame'         : getVideoFrame,
        'analyzeVideo'          : analyzeVideo,
        'transcodeVideo'        : transcodeVideo,
        'extractFrames'         : extractFrames,
        'deleteVideoAnalysis'   : deleteVideoAnalysis,
        'deleteVideoTranscoding': deleteVideoTranscoding,
        'deleteVideoFrames'     : deleteVideoFrames
    }
