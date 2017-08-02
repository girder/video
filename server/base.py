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

# This is imported from girder.plugins.jobs.constants, but cannot be done
# until after the plugin has been found and imported.  If using from an
# entrypoint, the load of this value must be deferred.
JobStatus = None


def _postUpload(event):
    """
    Called when a file is uploaded. We check the parent item to see if it is
    expecting a large image upload, and if so we register this file as the
    result image.
    """
    pass
    ## fileObj = event.info['file']
    ## # There may not be an itemId (on thumbnails, for instance)
    ## if not fileObj.get('itemId'):
    ##     return

    ## Item = ModelImporter.model('item')
    ## item = Item.load(fileObj['itemId'], force=True, exc=True)

    ## if item.get('largeImage', {}).get('expected') and (
    ##         fileObj['name'].endswith('.tiff') or
    ##         fileObj.get('mimeType') == 'image/tiff'):
    ##     if fileObj.get('mimeType') != 'image/tiff':
    ##         fileObj['mimeType'] = 'image/tiff'
    ##         ModelImporter.model('file').save(fileObj)
    ##     del item['largeImage']['expected']
    ##     item['largeImage']['fileId'] = fileObj['_id']
    ##     item['largeImage']['sourceName'] = 'tiff'
    ##     Item.save(item)


def updateJob(event):
    """
    Called when a job is saved, updated, or removed.  If this is a video
    job and it is ended, clean up after it.
    """
    global JobStatus
    if not JobStatus:
        from girder.plugins.jobs.constants import JobStatus

    job = (
        event.info['job']
        if event.name == 'jobs.job.update.after'
        else event.info
    )

    jobVideoData = job.get('meta', {}).get('video_plugin')
    if jobVideoData is None:
        return

    videoItemId = jobVideoData.get('itemId')
    videoFileId = jobVideoData.get('fileId')
    if videoItemId is None or videoFileId is None:
        return

    status = job['status']
    if event.name == 'model.job.remove' and status not in (
            JobStatus.ERROR, JobStatus.CANCELED, JobStatus.SUCCESS):
        status = JobStatus.CANCELED
    if status not in (JobStatus.ERROR, JobStatus.CANCELED, JobStatus.SUCCESS):
        return

    item = ModelImporter.model('item').load(videoItemId, force=True)
    if not item:
        return

    itemVideoData = item.get('video')
    if itemVideoData is None:
        return

    if itemVideoData['jobId'] != str(job['_id']):
        return

    # TODO(opadron): remove this after this section is finished
    print(
        'Found video item %s from job %s' %
        (videoItemId, str(job['_id'])))

    # if item.get('largeImage', {}).get('expected'):
    #     # We can get a SUCCESS message before we get the upload message, so
    #     # don't clear the expected status on success.
    #     if status != JobStatus.SUCCESS:
    #         del item['largeImage']['expected']

    # notify = item.get('largeImage', {}).get('notify')
    # msg = None
    # if notify:
    #     del item['largeImage']['notify']
    #     if status == JobStatus.SUCCESS:
    #         msg = 'Large image created'
    #     elif status == JobStatus.CANCELED:
    #         msg = 'Large image creation canceled'
    #     else:  # ERROR
    #         msg = 'FAILED: Large image creation failed'
    #     msg += ' for item %s' % item['name']
    # if (status in (JobStatus.ERROR, JobStatus.CANCELED) and
    #         'largeImage' in item):
    #     del item['largeImage']

    # ModelImporter.model('item').save(item)
    # if msg and event.name != 'model.job.remove':
    #     ModelImporter.model('job', 'jobs').updateJob(job, progressMessage=msg)


def checkForLargeImageFiles(event):
    pass
    ## file = event.info
    ## possible = False
    ## mimeType = file.get('mimeType')
    ## if mimeType in ('image/tiff', 'image/x-tiff', 'image/x-ptif'):
    ##     possible = True
    ## exts = file.get('exts')
    ## if exts and exts[-1] in ('svs', 'ptif', 'tif', 'tiff', 'ndpi'):
    ##     possible = True
    ## if not file.get('itemId') or not possible:
    ##     return
    ## if not ModelImporter.model('setting').get(
    ##         constants.PluginSettings.LARGE_IMAGE_AUTO_SET):
    ##     return
    ## item = ModelImporter.model('item').load(
    ##     file['itemId'], force=True, exc=False)
    ## if not item or item.get('largeImage'):
    ##     return
    ## imageItemModel = ModelImporter.model('image_item', 'large_image')
    ## try:
    ##     imageItemModel.createImageItem(item, file, createJob=False)
    ## except Exception:
    ##     # We couldn't automatically set this as a large image
    ##     logger.info('Saved file %s cannot be automatically used as a '
    ##                 'largeImage' % str(file['_id']))


def removeThumbnails(event):
    pass
    ## ModelImporter.model('image_item', 'large_image').removeThumbnailFiles(
    ##     event.info)


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
    version='0.1.0',
    dependencies={'worker'},
)
def load(info):
    from .rest import addItemRoutes

    addItemRoutes(info['apiRoot'].item)

    ModelImporter.model('item').exposeFields(
        level=AccessType.READ, fields='video')

    events.bind('data.process', 'video', _postUpload)
    events.bind('jobs.job.update.after', 'video', updateJob)
    events.bind('model.job.save', 'video', updateJob)
    events.bind('model.job.remove', 'video', updateJob)
    ## events.bind('model.folder.save.after', 'video',
    ##             invalidateLoadModelCache)
    ## events.bind('model.group.save.after', 'video',
    ##             invalidateLoadModelCache)
    ## events.bind('model.item.remove', 'video', invalidateLoadModelCache)
    events.bind('model.file.save.after', 'video',
                checkForLargeImageFiles)
    events.bind('model.item.remove', 'video', removeThumbnails)
