import time

from bson import ObjectId


class FAMongoId(ObjectId):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type='string')


lastTimeStampReturned = 0


def getTimeStamp():
    global lastTimeStampReturned
    ts = time.time()
    if ts <= lastTimeStampReturned:
        ts = lastTimeStampReturned + 1e-7
    lastTimeStampReturned = ts
    return ts
