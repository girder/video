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

from girder import events, plugin
from girder.constants import SettingDefault
from girder.models.model_base import ValidationException
from girder.utility import setting_utilities

from .constants import PluginSettings, VideoEvents

@setting_utilities.validator({
    'video.cacheId'
})
def validateSettings(doc):
    id = doc['value']
    pass

# Validators

@setting_utilities.validator({
    PluginSettings.VIDEO_SHOW_THUMBNAILS,
    PluginSettings.VIDEO_SHOW_VIEWER,
    PluginSettings.VIDEO_AUTO_SET,
})
def validateBoolean(doc):
    val = doc['value']
    if str(val).lower() not in ('false', 'true', ''):
        raise ValidationException('%s must be a boolean.' % doc['key'], 'value')
    doc['value'] = (str(val).lower() != 'false')


@setting_utilities.validator({
    PluginSettings.VIDEO_SHOW_EXTRA,
    PluginSettings.VIDEO_SHOW_EXTRA_ADMIN,
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
    PluginSettings.VIDEO_MAX_THUMBNAIL_FILES,
    PluginSettings.VIDEO_MAX_SMALL_IMAGE_SIZE,
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
    PluginSettings.VIDEO_DEFAULT_VIEWER
})
def validateDefaultViewer(doc):
    doc['value'] = str(doc['value']).strip()


# Defaults

# Defaults that have fixed values can just be added to the system defaults
# dictionary.
SettingDefault.defaults.update({
    PluginSettings.VIDEO_SHOW_THUMBNAILS: True,
    PluginSettings.VIDEO_SHOW_VIEWER: True,
    PluginSettings.VIDEO_AUTO_SET: True,
    PluginSettings.VIDEO_MAX_THUMBNAIL_FILES: 10,
    PluginSettings.VIDEO_MAX_SMALL_IMAGE_SIZE: 4096,
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
    from .rest import addFileRoutes, Video
    from .helpers import initHelpers

    apiRoot = info['apiRoot']
    fileApi = apiRoot.file

    initHelpers(fileApi)  # initialize the helper functions
    routeTable = addFileRoutes(fileApi)  # amend the file api with new routes
    apiRoot.video = Video()

    # warning: awful hacks, below!  Upstreaming fixes that make these
    # workarounds unecessary should be made a priority.
    from . import dirty_hacks as hax
    hax.addAttachmentIndices('file')
    hax.addAttachmentIndices('item')
    hax.patchItemModel()
    hax.patchUploadModel()

    from . import event_handlers as handlers
    events.bind(VideoEvents.FILE_UPLOADED, 'video', handlers.onFileUpload)
    events.bind(VideoEvents.FILE_DELETED , 'video', handlers.onFileDelete)
    events.bind(VideoEvents.FRAME_EXTRACT, 'video', handlers.onFrames)
    events.bind(VideoEvents.ANALYZE      , 'video', handlers.onAnalyze)
