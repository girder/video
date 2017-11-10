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


# Constants representing the setting keys for this plugin
class PluginSettings:
    VIDEO_SHOW_THUMBNAILS = 'video.show_thumbnails'
    VIDEO_SHOW_EXTRA = 'video.show_extra'
    VIDEO_SHOW_EXTRA_ADMIN = 'video.show_extra_admin'
    VIDEO_SHOW_VIEWER = 'video.show_viewer'
    VIDEO_DEFAULT_VIEWER = 'video.default_viewer'
    VIDEO_AUTO_SET = 'video.auto_set'
    VIDEO_MAX_THUMBNAIL_FILES = 'video.max_thumbnail_files'
    VIDEO_MAX_SMALL_IMAGE_SIZE = 'video.max_small_image_size'


class JobStatus:
    """Deferred loading of Girder's JobStatus constants"""
    def __init__(self):
        self._js = None

    @property
    def js(self):
        if not self._js:
            from girder.plugins.jobs.constants import JobStatus as JS
            self._js = JS
        return self._js

    def __getattr__(self, k):
        return getattr(self.js, k)

JobStatus = JobStatus()

class VideoModelType:
    """Enum type for Video models"""
    FILE   = 'FILE'
    FORMAT = 'FORMAT'
    FRAME  = 'FRAME'
    JOB    = 'JOB'

