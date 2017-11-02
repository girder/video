
from girder.constants import AccessType
from girder.models.model_base import AccessControlledModel, ModelImporter

from ..utils import objectIdOrNone


class Videojob(AccessControlledModel):
    def initialize(self):
        self.name = 'videojob'
        self.ensureIndices(['fileId', 'sourceFileId'])

        self.exposeFields(level=AccessType.READ, fields={
            '_id', 'type', 'fileId', 'sourceFileId',
            'formatName', 'jobId', 'userId'})

    def validate(self, data):
        return data

    def createVideojob(
            self, fileId=None, type=None, sourceFileId=None,
            formatName=None, jobId=None, save=True):
        videojob = {
            'type': type,
            'fileId': objectIdOrNone(fileId),
            'sourceFileId': objectIdOrNone(sourceFileId),
            'formatName': formatName,
            'jobId': objectIdOrNone(jobId)
        }

        file = ModelImporter.model('file').load(
                (sourceFileId or fileId),
                level=AccessType.READ)

        user = ModelImporter.model('user').load(
                file['creatorId'], level=AccessType.READ)

        if user:
            videojob['userId'] = objectIdOrNone(user['_id'])
            self.setUserAccess(videojob, user=user, level=AccessType.WRITE)
        else:
            videojob['userId'] = None

        if save:
            videojob = self.save(videojob)

        return videojob

