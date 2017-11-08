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

try:
    from queue import Queue
except ImportError:  # PYTHON 2
    from Queue import Queue

import json

from bson.objectid import ObjectId

from girder import events, plugin, logger
from girder.constants import AccessType, SettingDefault
from girder.models.model_base import ModelImporter, \
                                     GirderException, \
                                     ValidationException
from girder.utility import setting_utilities

from . import constants

JobStatus = constants.JobStatus
videoUtilities = None  # set at plugin load
frameJobArgQueue = Queue()  # nasty hack/abuse of events
analyzeJobArgQueue = Queue()  # nasty hack/abuse of events

@setting_utilities.validator({
    'video.cacheId'
})
def validateSettings(doc):
    id = doc['value']
    pass

def _onFrames(event):
    sourceFileId, numFrames, user, formatId = frameJobArgQueue.get()
    videoUtilities['extractFrames'](
        sourceFileId, numFrames, formatId=formatId, user=user)

def _onAnalyze(event):
    sourceFileId, user, formatId = analyzeJobArgQueue.get()
    videoUtilities['analyzeVideo'](
        sourceFileId, force=True, formatId=formatId, user=user)

def _deleteVideoData(event):
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

def _postUpload(event):
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
    referencedFileId = ObjectId(payload['fileId'])
    formatId = ObjectId(payload['formatId']) if payload['formatId'] else None
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

    if payload['dataType'] == 'analysis':
        vData = videoUtilities['getVideoData'](
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
            frameJobArgQueue.put(
                (referencedFileId, numFrames, user, formatId))
            events.daemon.trigger('frameExtract', 'video', formatId)

    elif payload['dataType'] == 'frame':
        frameIndex = payload['index']

        vData = videoModel.createVideoFrame(
            fileId=uploadedFile['_id'],
            sourceFileId=referencedFileId,
            formatId=formatId,
            index=frameIndex,
            save=True)

    elif payload['dataType'] == 'transcoding':
        vData = videoUtilities['getVideoData'](
                fileId=referencedFileId,
                formatId=formatId,
                user=event.info['currentUser'],
                ensure=True)

        update = { 'fileId': uploadedFile['_id'] }
        vData.update(update)

        videoModel.save(vData)

        analyzeJobArgQueue.put(
            (referencedFileId, event.info['currentUser'], formatId))
        events.daemon.trigger('videoAnalyze', 'video', None)


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


# Monkey-patch the Item model
#
# Since we use girder worker to generate data that we'd prefer to keep hidden,
# and Girder currently does not support uploading directly into a hidden file,
# we figured the best workaround is to extend to items the same attaching
# semantics that have been recently added to files, and to use a hidden item as
# the target for all uploads.
#
# TODO(opadron): remove this once better attaching support is merged into
#                master.
def itemCreateItem(self, *args, **kwargs):
    creator = kwargs.get('creator', (args + (None,)*2)[1])

    attachedToId = kwargs.get('attachedToId')
    attachedToType = kwargs.get('attachedToType')

    if attachedToId and attachedToType:
        attachedToId = ObjectId(attachedToId)
        import datetime

        now = datetime.datetime.utcnow()

        hidden = kwargs.get('hidden', False)

        if not isinstance(creator, dict) or '_id' not in creator:
            # Internal error -- this shouldn't be called without a user.
            raise GirderException('Creator must be a user.',
                                  'girder.models.item.creator-not-user')

        doc = {
            'name': '',
            'description': '',
            'creatorId': creator['_id'],
            'baseParentType': None,
            'baseParentId': None,
            'attachedToId': attachedToId,
            'attachedToType': attachedToType,
            'created': now,
            'updated': now,
            'size': 0
        }

        if hidden:
            doc = self.collection.find_and_modify(
                { 'creatorId': creator['_id'],
                  'attachedToId': attachedToId,
                  'attachedToType': attachedToType },
                { '$set': doc },
                new=True,
                upsert=True
            )
            return doc

        return self.save(doc)

    return self._originalCreateItem(*args, **kwargs)


def createHiddenItem(self, user):
    return self.createItem(attachedToId=user['_id'],
                           attachedToType='user',
                           creator=user,
                           hidden=True)


def itemIsOrphan(self, item):
    return (
        ModelImporter('file').isOrphan(item) if item.get('attachedToId') else
        self._originalIsOrphan(item)
    )


def itemValidate(self, doc):
    attachedToId = doc.get('attachedToId')
    attachedToType = doc.get('attachedToType')

    if attachedToId and attachedToType:
        doc['lowerName'] = doc['name'].lower()
        return doc

    return self._originalValidate(doc)


def createUpload(self, *args, **kwargs):
    reference = kwargs.get('reference')

    customPayload = None
    if reference and reference.startswith('override:'):
        try:
            customPayload = json.loads(reference[9:])
        except:
            pass

    if customPayload:
        kwargs.update(customPayload)

    return self._originalCreateUpload(*args, **kwargs)


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

    info['apiRoot'].video = Video()

    routeTable = addFileRoutes(info['apiRoot'].file)
    videoUtilities = routeTable['utilities']

    ModelImporter.model('file').ensureIndices([
        'attachedToType', 'attachedToId'])

    ModelImporter.model('item').ensureIndices([
        'attachedToType', 'attachedToId'])

    from girder.models.item import Item
    (
        Item._originalCreateItem, Item.createItem,
        Item._originalIsOrphan  , Item.isOrphan  ,
        Item._originalValidate  , Item.validate
    ) = (
        Item.createItem, itemCreateItem,
        Item.isOrphan  , itemIsOrphan  ,
        Item.validate  , itemValidate
    )

    Item.createHiddenItem = createHiddenItem

    from girder.models.upload import Upload
    (
        Upload._originalCreateUpload, Upload.createUpload
    ) = (
        Upload.createUpload         , createUpload
    )

    events.bind('data.process', 'video', _postUpload)
    events.bind('model.file.remove', 'video', _deleteVideoData)
    events.bind('frameExtract', 'video', _onFrames)
    events.bind('videoAnalyze', 'video', _onAnalyze)

