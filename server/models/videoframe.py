
from girder.constants import AccessType
from girder.models.model_base import AccessControlledModel, ModelImporter

from ..utils import objectIdOrNone


class Videoframe(AccessControlledModel):
    def initialize(self):
        self.name = 'videoframe'
        self.ensureIndices(['fileId', 'sourceFileId', 'index'])

        self.exposeFields(level=AccessType.READ, fields={
            '_id', 'fileId', 'sourceFileId',
            'formatName', 'index', 'itemId', 'userId'})

    def validate(self, data):
        return data

    def createVideoframe(
            self, fileId=None, sourceFileId=None,
            formatName=None, index=None, itemId=None, save=True):

        videoframe = {
            'fileId': objectIdOrNone(fileId),
            'sourceFileId': objectIdOrNone(sourceFileId),
            'formatName': formatName,
            'index': index,
            'itemId': objectIdOrNone(itemId)
        }

        file = ModelImporter.model('file').load(
                (sourceFileId or fileId), level=AccessType.READ)

        user = ModelImporter.model('user').load(
                file['creatorId'], level=AccessType.READ)

        if user:
            videoframe['userId'] = objectIdOrNone(user['_id'])
            self.setUserAccess(videoframe, user=user, level=AccessType.WRITE)
        else:
            videoframe['userId'] = None

        if save:
            videoframe = self.save(videoframe)

        return videoframe

