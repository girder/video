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

from girder import events, plugin, logger
from girder.constants import AccessType, SettingDefault
from girder.models.model_base import ModelImporter, ValidationException
from girder.utility import setting_utilities

from . import constants

JobStatus = constants.JobStatus
videoUtilities = None  # set at plugin load
frameJobArgs = None  # nasty hack/abuse of events

@setting_utilities.validator({
    'video.cacheId'
})
def validateSettings(doc):
    id = doc['value']
    pass

def _onFrames(event):
    global frameJobArgs
    if frameJobArgs is not None:
        sourceFileId = frameJobArgs.get('sourceFileId')
        numFrames = frameJobArgs.get('numFrames')
        user = frameJobArgs.get('currentUser')
        frameJobArgs = None

        videoUtilities['extractFrames'](sourceFileId, numFrames, user=user)

def _deleteVideoData(event):
    file = event.info
    videodataModel = ModelImporter.model('videodata', 'video')
    vData = videodataModel.findOne({ 'fileId': file['_id'] })
    if vData:
        videodataModel.remove(vData)

def _postUpload(event):
    """
    Called when a file is uploaded. If the file was created by the video
    plugin's processing jobs, they are handled here.
    """
    global frameJobArgs
    reference = event.info.get('reference', '')
    if not reference.startswith('videoPlugin'):
        return

    if reference.startswith('videoPlugin:analysis:meta:'):
        fileModel = ModelImporter.model('file')
        videodataModel = ModelImporter.model('videodata', 'video')
        sourceFileId = reference.split(':')[-1]
        vData = videoUtilities['getVideoData'](
                id=sourceFileId,
                level=AccessType.WRITE,
                user=event.info['currentUser'])

        with fileModel.open(event.info['file']) as f:
            vData['metadata'] = json.load(f)

        videodataModel.save(vData)

        numFrames = int(
            vData
                .get('metadata', {})
                .get('video', {})
                .get('frameCount', 0))

        if numFrames:
            frameJobArgs = {
                'sourceFileId': sourceFileId,
                'numFrames': numFrames,
                'currentUser': event.info['currentUser']
            }
            events.daemon.trigger('frameExtract', 'video', None)

    elif reference.startswith('videoPlugin:analysis:frame:'):
        fileModel = ModelImporter.model('file')
        videodataModel = ModelImporter.model('videodata', 'video')
        videoframeModel = ModelImporter.model('videoframe', 'video')
        tokens = reference.split(':')[3:]

        sourceFileId = None
        format = None
        frameIndex = None

        if len(tokens) == 2:
            sourceFileId, frameIndex = tokens
        else:
            format, sourceFileId, frameIndex = tokens[:3]

        frameIndex = int(frameIndex)

        if format is None:
            videoframeModel.createVideoframe(
                fileId=sourceFileId, index=frameIndex,
                itemId=event.info['file']['itemId'], save=True)
        else:
            videoframeModel.createVideoframe(
                sourceFileId=sourceFileId, index=frameIndex, formatName=format,
                itemId=event.info['file']['itemId'], save=True)


# Validators

@setting_utilities.validator({
    constants.PluginSettings.VIDEO_SHOW_THUMBNAILS,
    constants.PluginSettings.VIDEO_SHOW_VIEWER,
    constants.PluginSettings.VIDEO_AUTO_SET,
})
def validateBoolean(doc):
    val = doc['value']
    if str(val).lower() not in ('false', 'true', ''):
        raise ValidationException('%s must be a boolean.' % doc['key'], 'value')
    doc['value'] = (str(val).lower() != 'false')


@setting_utilities.validator({
    constants.PluginSettings.VIDEO_SHOW_EXTRA,
    constants.PluginSettings.VIDEO_SHOW_EXTRA_ADMIN,
})
def validateDictOrJSON(doc):
    val = doc['value']
    try:
        if isinstance(val, dict):
            doc['value'] = json.dumps(val)
        elif val is None or val.strip() == '':
            doc['value'] = ''
        else:
            parsed = json.loads(val)
            if not isinstance(parsed, dict):
                raise ValidationException('%s must be a JSON object.' % doc['key'], 'value')
            doc['value'] = val.strip()
    except (ValueError, AttributeError):
        raise ValidationException('%s must be a JSON object.' % doc['key'], 'value')


@setting_utilities.validator({
    constants.PluginSettings.VIDEO_MAX_THUMBNAIL_FILES,
    constants.PluginSettings.VIDEO_MAX_SMALL_IMAGE_SIZE,
})
def validateNonnegativeInteger(doc):
    val = doc['value']
    try:
        val = int(val)
        if val < 0:
            raise ValueError
    except ValueError:
        raise ValidationException('%s must be a non-negative integer.' % (
            doc['key'], ), 'value')
    doc['value'] = val


@setting_utilities.validator({
    constants.PluginSettings.VIDEO_DEFAULT_VIEWER
})
def validateDefaultViewer(doc):
    doc['value'] = str(doc['value']).strip()


# Defaults

# Defaults that have fixed values can just be added to the system defaults
# dictionary.
SettingDefault.defaults.update({
    constants.PluginSettings.VIDEO_SHOW_THUMBNAILS: True,
    constants.PluginSettings.VIDEO_SHOW_VIEWER: True,
    constants.PluginSettings.VIDEO_AUTO_SET: True,
    constants.PluginSettings.VIDEO_MAX_THUMBNAIL_FILES: 10,
    constants.PluginSettings.VIDEO_MAX_SMALL_IMAGE_SIZE: 4096,
})


# Configuration and load

@plugin.config(
    name='Video',
    description='Process, serve, and display videos.',
    version='0.2.0',
    dependencies={'worker'},
)
def load(info):
    global videoUtilities
    from .rest import addFileRoutes

    routeTable = addFileRoutes(info['apiRoot'].file)
    videoUtilities = routeTable['utilities']

    ModelImporter.model('file').exposeFields(
        level=AccessType.READ, fields='video')

    events.bind('data.process', 'video', _postUpload)
    events.bind('model.file.remove', 'video', _deleteVideoData)
    events.bind('frameExtract', 'video', _onFrames)

