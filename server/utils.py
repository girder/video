
from bson.objectid import ObjectId

def objectIdOrNone(arg):
    return ObjectId(arg) if arg else None

def convertOrNone(x, func):
    try:
        x = func(x)
    except ValueError, TypeError:
        x = None
    return x


def floatOrNone(x):
    return convertOrNone(x, float)


def intOrNone(x):
    return convertOrNone(x, int)


