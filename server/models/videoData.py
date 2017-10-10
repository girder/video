
from girder.constants import AccessType
from girder.models.model_base import AccessControlledModel, ModelImporter


class Videodata(AccessControlledModel):
    def initialize(self):
        self.name = 'videodata'
        self.ensureIndices(['fileId'])

        self.exposeFields(level=AccessType.READ, fields={
            '_id', 'fileId', 'sourceFileId', 'cacheId',
            'sourceFormat', 'formats', 'userId'})

    def validate(self, data):
        return data

    def createVideoData(self, fileId, save=True):
        videodata = {
            'fileId': fileId,
            'sourceFileId': None,
            'sourceFormat': {},
            'formats': [],
        }

        file = ModelImporter.model('file').load(
                fileId, level=AccessType.READ)
        user = ModelImporter.model('user').load(
                file['creatorId'], level=AccessType.READ)

        if user:
            videodata['userId'] = user['_id']
            self.setUserAccess(videodata, user=user, level=AccessType.WRITE)
        else:
            videodata['userId'] = None

        if save:
            videodata = self.save(videodata)

        return videodata

