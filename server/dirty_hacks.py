def patchItemModel():
    """
    Monkey-patch the Item model

    The idea is to use girder worker to generate new data (transcoded videos and
    extracts frames) while having the data completely hidden from end-to-end.

    Since Girder currently does not support uploading directly into a hidden
    file, items are extended the same attaching semantics that have been
    recently added to files as a workaround.  A per-user hidden item is then
    used as the target for all uploads.

    TODO(opadron): remove this once better attaching support is merged in
    """
    from girder.models.model_base import GirderException
    from girder.models.item import Item
    from .utils import objectIdOrNone

    def createItem(self, *args, **kwargs):
        """Hacked version of itemModel.createItem()."""
        creator = kwargs.get('creator', (args + (None,)*2)[1])

        attachedToId = kwargs.get('attachedToId')
        attachedToType = kwargs.get('attachedToType')

        if attachedToId and attachedToType:
            attachedToId = objectIdOrNone(attachedToId)
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

    def isOrphan(self, item):
        """
        Hacked version of itemModel.isOrphan().

        Uses the file model's logic if the item is hidden.
        """
        return (
            self.model('file').isOrphan(item) if item.get('attachedToId') else
            self._originalIsOrphan(item)
        )

    def validate(self, doc):
        """
        Hacked version of itemModel.validate()

        Adds logic to tolerate hidden items.
        """
        attachedToId = doc.get('attachedToId')
        attachedToType = doc.get('attachedToType')

        if attachedToId and attachedToType:
            doc['lowerName'] = doc['name'].lower()
            return doc

        return self._originalValidate(doc)

    def createHiddenItem(self, user):
        """
        Wrapper around hacked itemModel.createItem() for ensuring the user's
        "secret" hidden item.
        """
        return self.createItem(attachedToId=user['_id'],
                               attachedToType='user',
                               creator=user,
                               hidden=True)

    Item._originalCreateItem = Item.createItem
    Item._originalIsOrphan   = Item.isOrphan
    Item._originalValidate   = Item.validate

    Item.createItem       = createItem
    Item.isOrphan         = isOrphan
    Item.validate         = validate
    Item.createHiddenItem = createHiddenItem

def addAttachmentIndices(model):
    """Ensure attachment indices for the given model."""
    from girder.models.model_base import ModelImporter
    ModelImporter.model(model).ensureIndices([
        'attachedToType', 'attachedToId'])

def patchUploadModel():
    """
    Monkey-patch the Upload model

    Hijacks the createUpload() so that if the reference string starts with
    "override:", and is followed by a JSON object, the object is deserialized
    and is used to override the passed keyword arguments.  This added layer
    accomplishes two important things:
      1 - Although attachment handling logic has been added to the original
          createUpload(), the relevant options have not been exposed in the
          "POST /file" endpoint.  This layer allows girder worker to take
          advantage of the functionality by going through the reference string.

      2 - It adds an injection point in which the assetstore can be set before
          the actual upload, allowing the future configuration of how the
          video plugin stores transcoded videos and extracted frames.
    """
    import json
    from girder.models.upload import Upload

    def createUpload(self, *args, **kwargs):
        """Hacked version of uploadModel.createUpload()"""
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

    Upload._originalCreateUpload = Upload.createUpload
    Upload.createUpload          = createUpload
