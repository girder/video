
from girder.constants import AccessType
from girder.models.model_base import AccessControlledModel, ModelImporter

from ..utils import objectIdOrNone


class Videodata(AccessControlledModel):
    def initialize(self):
        self.name = 'videodata'
        self.ensureIndices(['fileId', 'sourceFileId'])

        self.exposeFields(level=AccessType.READ, fields={
            '_id', 'fileId', 'sourceFileId',
            'formatName', 'cacheId', 'metadata'})

    def validate(self, data):
        return data

    def createVideodata(
            self, fileId=None, sourceFileId=None, formatName=None,
            cacheId=None, metadata=None, save=True):

        videodata = {
            'fileId': objectIdOrNone(fileId),
            'sourceFileId': objectIdOrNone(sourceFileId or fileId),
            'formatName': formatName,
            'cacheId': objectIdOrNone(cacheId),
            'metadata': (metadata or {})
        }

        file = ModelImporter.model('file').load(
                (sourceFileId or fileId), level=AccessType.READ)

        user = ModelImporter.model('user').load(
                file['creatorId'], level=AccessType.READ)

        if user:
            videodata['userId'] = objectIdOrNone(user['_id'])
            self.setUserAccess(videodata, user=user, level=AccessType.WRITE)
        else:
            videodata['userId'] = None

        if save:
            videodata = self.save(videodata)

        return videodata

