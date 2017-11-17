#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
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
#############################################################################

import json

from girder import events
from girder.models.model_base import ModelImporter

from .helpers import helpers
from .utils import objectIdOrNone
from .constants import VideoEnum, VideoEvents


def onFrames(event):
    data = event.info
    helpers.extractFrames(
        data['fileId'], data['numFrames'],
        formatId=data['formatId'], user=data['user'])

def onAnalyze(event):
    data = event.info
    helpers.analyzeVideo(
        data['fileId'], formatId=data['formatId'],
        user=data['user'], force=True)

def onFileDelete(event):
    file = event.info
    videoModel = ModelImporter.model('video', 'video')

    # TODO(opadron): remove all video entries with fileId == file['_id']
    #                    - EXCEPT for those with sourceFileId == file['_id']
    #
    # TODO(opadron): for all video entries with sourceFileId == file['_id']:
    #                    - delete the file identified by "fileId"
    #                    - the above step should trigger this handler
    #                      recursively.
    # vData = videoModel.findOne({ 'fileId': file['_id'] })
    # if vData:
    #     videodataModel.remove(vData)

def onFileUpload(event):
    """
    Called when a file is uploaded. If the file was created by the video
    plugin's processing jobs, they are handled here.
    """
    reference = event.info.get('reference', '')
    if not reference.startswith('videoPlugin:'):
        return

    payload = {}
    try:
        payload = json.loads(reference[12:])
    except:
        pass

    uploadedFile = event.info['file']
    referencedFileId = objectIdOrNone(payload['fileId'])
    formatId = objectIdOrNone(payload['formatId'])
    user = event.info['currentUser']

    fileModel = ModelImporter.model('file')
    videoModel = ModelImporter.model('video', 'video')

    # Detach the uploaded file from the staging item and
    # reattach it to the referenced file.
    uploadedFile.update({
        'itemId': None,
        'attachedToId': referencedFileId,
        'attachedToType': 'file',
    })

    uploadedFile = fileModel.save(uploadedFile)

    if payload['dataType'] == VideoEnum.ANALYSIS:
        vData = helpers.getVideoData(
                fileId=referencedFileId,
                formatId=formatId,
                user=user,
                ensure=True)

        with fileModel.open(event.info['file']) as f:
            data = json.load(f)
            vData.update(data)

        vData = videoModel.save(vData)

        numFrames = int(vData.get('videoFrameCount', 0))
        if numFrames:
            events.daemon.trigger(
                VideoEvents.FRAME_EXTRACT,
                {
                    'fileId': referencedFileId,
                    'numFrames': numFrames,
                    'user': user,
                    'formatId': formatId
                }
            )

    elif payload['dataType'] == VideoEnum.FRAME_EXTRACTION:
        frameIndex = payload['index']

        vData = videoModel.createVideoFrame(
            fileId=uploadedFile['_id'],
            sourceFileId=referencedFileId,
            formatId=formatId,
            index=frameIndex,
            save=True)

    elif payload['dataType'] == VideoEnum.TRANSCODING:
        vData = helpers.getVideoData(
                fileId=referencedFileId,
                formatId=formatId,
                user=event.info['currentUser'],
                ensure=True)

        update = { 'fileId': uploadedFile['_id'] }
        vData.update(update)

        videoModel.save(vData)

        events.daemon.trigger(
            VideoEvents.ANALYZE,
            {
                'fileId': referencedFileId,
                'user': event.info['currentUser'],
                'formatId': formatId
            }
        )
