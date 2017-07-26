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

import cherrypy

from girder import logger
from girder.api import access
from girder.api.describe import describeRoute, Description
from girder.api.rest import Resource, loadmodel, filtermodel, RestException
from girder.constants import AccessType, SortDir
from girder.models.model_base import ValidationException


class VideoResource(Resource):

    def __init__(self):
        super(VideoResource, self).__init__()

        self.resourceName = 'video'
        self.route('GET', ('item', ':id', 'video'), self.getVideoMetadata)
        self.route('PUT', ('item', ':id', 'video'), self.processVideo)
        self.route('DELETE', ('item', ':id', 'video'),
                self.deleteProcessedVideo)
        self.route('GET', ('item', ':id', 'video', 'frame'), self.getVideoFrame)

    @describeRoute(
        Description('Return video metadata if it exists.')
        .param('id', 'Id of the item.', paramType='path')
        .errorResponse()
        .errorResponse('Read access was denied on the item.', 403)
    )
    @access.public
    # @filtermodel(model='annotation', plugin='video')
    def getVideoMetadata(self, params):
        limit, offset, sort = self.getPagingParameters(params, 'lowerName')
        query = {}
        item = self.model('item').load(params.get('itemId'), force=True)
        self.model('item').requireAccess(
            item, user=self.getCurrentUser(), level=AccessType.READ)
        query['itemId'] = item['_id']

    @describeRoute(
        Description('Create a girder-worker job to process the given video.')
        .param('id', 'Id of the item.', paramType='path')
        .param('fileId', 'Id of the file to use as the item.', required=False)
        .errorResponse()
        .errorResponse('Read access was denied on the item.', 403)
    )
    @access.public
    def processVideo(self, params):
        pass

    @describeRoute(
        Description('Delete the processed results from the given video.')
        .param('id', 'Id of the item.', paramType='path')
        .errorResponse()
        .errorResponse('Write access was denied on the item.', 403)
    )
    @access.public
    def deleteProcessedVideo(self, params):
        pass

    @describeRoute(
        Description('Get a single frame from the given video.')
        .param('id', 'Id of the item.', paramType='path')
        .param('time', 'Point in time from which to sample the frame.',
            required=True)
        .errorResponse()
        .errorResponse('Read access was denied on the item.', 403)
    )
    def getVideoFrame(self, params):
        pass

