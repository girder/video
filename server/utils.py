
from bson.objectid import ObjectId

def objectIdOrNone(arg):
    return ObjectId(arg) if arg else None

