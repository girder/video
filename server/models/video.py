
from girder.constants import AccessType
from girder.models.model_base import Model

from ..constants import VideoEnum
from ..utils import objectIdOrNone, floatOrNone, intOrNone

def _handleCreateOperation(self, args, creatorFunc=None):
    fileId       = objectIdOrNone(args.get('fileId'))
    formatId     = objectIdOrNone(args.get('formatId'))
    jobId        = objectIdOrNone(args.get('jobId'))
    sourceFileId = objectIdOrNone(args.get('sourceFileId'))

    if sourceFileId is None:
        sourceFileId = fileId

    audioBitRate       = floatOrNone(args.get('audioBitRate'))
    audioSampleRate    = floatOrNone(args.get('audioSampleRate'))
    duration           = floatOrNone(args.get('duration'))
    videoBitRate       = floatOrNone(args.get('videoBitRate'))
    videoFrameRate     = floatOrNone(args.get('videoFrameRate'))

    audioChannelCount  = intOrNone(args.get('audioChannelCount'))
    videoHeight        = intOrNone(args.get('videoHeight'))
    videoFrameCount    = intOrNone(args.get('videoFrameCount'))
    videoWidth         = intOrNone(args.get('videoWidth'))

    index              = intOrNone(args.get('index'))

    audioChannelLayout = args.get('audioChannelLayout')
    audioSampleFormat  = args.get('audioSampleFormat')

    name    = args.get('name')
    type    = args.get('type')
    jobType = args.get('jobType')

    audioCodec = args.get('audioCodec')
    videoCodec = args.get('videoCodec')

    if type is None and creatorFunc is None:
        raise ValueError(
            'at least one of either type or creatorFunc must be defined')

    if creatorFunc is None:
        creatorFunc = (
            _createVideoFile   if type == VideoEnum.FILE   else
            _createVideoFormat if type == VideoEnum.FORMAT else
            _createVideoFrame  if type == VideoEnum.FRAME  else
            _createVideoJob    if type == VideoEnum.JOB    else
            None
        )

    if creatorFunc is None:
        raise ValueError("unrecognized video entry type: '{}'".format(type))

    if type is None:
        type = (
            VideoEnum.FILE   if creatorFunc == _createVideoFile   else
            VideoEnum.FORMAT if creatorFunc == _createVideoFormat else
            VideoEnum.FRAME  if creatorFunc == _createVideoFrame  else
            VideoEnum.JOB    if creatorFunc == _createVideoJob    else
            None
        )

    if type is None:
        raise ValueError(
            "unrecognized creatorFunc: '{}'".format(
                getattr(creatorFunc, '__name__', '[unknown]')))

    newArgs = {}
    newArgs.update(args)
    newArgs.update({
        'type'               : type,
        'name'               : name,
        'fileId'             : fileId,
        'formatId'           : formatId,
        'index'              : index,
        'jobId'              : jobId,
        'jobType'            : jobType,
        'sourceFileId'       : sourceFileId,
        'audioBitRate'       : audioBitRate,
        'audioChannelCount'  : audioChannelCount,
        'audioChannelLayout' : audioChannelLayout,
        'audioCodec'         : audioCodec,
        'audioSampleFormat'  : audioSampleFormat,
        'audioSampleRate'    : audioSampleRate,
        'duration'           : duration,
        'videoBitRate'       : videoBitRate,
        'videoCodec'         : videoCodec,
        'videoFrameRate'     : videoFrameRate,
        'videoHeight'        : videoHeight,
        'videoFrameCount'    : videoFrameCount,
        'videoWidth'         : videoWidth
    })

    result = creatorFunc(self, **newArgs)
    result.update({ 'type': type })
    if newArgs.get('save'):
        result = self.save(result)

    return result


def _createVideoFile(
    self, fileId=None, sourceFileId=None, formatId=None, audioBitRate=None,
    audioChannelCount=None, audioChannelLayout=None, audioSampleFormat=None,
    audioSampleRate=None, duration=None, videoWidth=None, videoHeight=None,
    videoFrameCount=None, videoBitRate=None, videoFrameRate=None, **kwds):
    """
    Create a video entry for a video file.

    :param fileId: ID of the file as stored within Girder
    :type fileId: str
    :param sourceFileId: ID of the source file from which this file is derived,
                         or fileId if it is the original source file.
    :type sourceFileId: str
    :param formatId: ID of the format that this file has been transcoded into,
                     or None if fileId is the original source file.
    :type formatId: str
    :param audioBitRate: Audio bit rate -- detected by video plugin
    :type audioBitRate: float
    :param audioChannelCount: Number of audio channels -- detected by video
                              plugin
    :type audioChannelCount: int
    :param audioChannelLayout: Audio channel layout -- detected by video plugin
    :type audioChannelLayout: str
    :param audioSampleFormat: Audio sample format -- detected by video plugin
    :type audioSampleFormat: str
    :param audioSampleRate: Audio sample rate -- detected by video plugin
    :type audioSampleRate: float
    :param duration: Video duration in seconds -- detected by video plugin
    :type duration: float
    :param videoWidth: Video width in pixels -- detected by video plugin
    :type videoWidth: int
    :param videoHeight: Video height in pixels -- detected by video plugin
    :type videoHeight: int
    :param videoFrameCount: Number of frames in the video -- detected by video
                            plugin
    :type videoFrameCount: int
    :param videoBitRate: Video bit rate -- detected by video plugin
    :type videoBitRate: float
    :param videoFrameRate: Video frame rate -- detected by video plugin
    :type videoFrameRate: float
    """

    return {
        'fileId'             : fileId,
        'formatId'           : formatId,
        'sourceFileId'       : sourceFileId,
        'audioBitRate'       : audioBitRate,
        'audioChannelCount'  : audioChannelCount,
        'audioChannelLayout' : audioChannelLayout,
        'audioSampleFormat'  : audioSampleFormat,
        'audioSampleRate'    : audioSampleRate,
        'duration'           : duration,
        'videoBitRate'       : videoBitRate,
        'videoFrameCount'    : videoFrameCount,
        'videoFrameRate'     : videoFrameRate,
        'videoHeight'        : videoHeight,
        'videoWidth'         : videoWidth
    }

def _createVideoFormat(
    self, name=None, audioBitRate=None, audioChannelCount=None,
    audioChannelLayout=None, audioCodec=None, audioSampleFormat=None,
    audioSampleRate=None, videoWidth=None, videoHeight=None, videoBitRate=None,
    videoCodec=None, videoFrameRate=None, **kwds):
    """
    Create a video entry for a video transcoding format.

    :param name: Name of the format
    :type name: str
    :param audioBitRate: Desired audio bit rate, or None for source
    :type audioBitRate: float
    :param audioChannelCount: Desired number of audio channels, or None for
                              source
    :type audioChannelCount: int
    :param audioChannelLayout: Desired audio channel layout, or None for source
    :type audioChannelLayout: str
    :param audioCodec: Desired audio codec, or None for the default audio codec
    :type audioCodec: str
    :param audioSampleFormat: Desired audio sample format, or None for source
    :type audioSampleFormat: str
    :param audioSampleRate: Desired audio sample rate, or None for source
    :type audioSampleRate: float
    :param videoWidth: Desired video width in pixels, or None for source
    :type videoWidth: int
    :param videoHeight: Desired video height in pixels, or None for source
    :type videoHeight: int
    :param videoBitRate: Desired video bit rate, or None for source
    :type videoBitRate: float
    :param videoCodec: Desired video codec, or None for the default video codec
    :type videoCodec: str
    :param videoFrameRate: Desired video frame rate, or None for source
    :type videoFrameRate: float
    """

    return {
        'name'              : name,
        'audioBitRate'      : audioBitRate,
        'audioChannelCount' : audioChannelCount,
        'audioChannelLayout': audioChannelLayout,
        'audioCodec'        : audioCodec,
        'audioSampleFormat' : audioSampleFormat,
        'audioSampleRate'   : audioSampleRate,
        'videoBitRate'      : videoBitRate,
        'videoCodec'        : videoCodec,
        'videoFrameRate'    : videoFrameRate,
        'videoHeight'       : videoHeight,
        'videoWidth'        : videoWidth
    }


def _createVideoFrame(
    self, fileId=None, sourceFileId=None, formatId=None, index=None, **kwds):
    """
    Create a video entry for an extracted video frame.

    :param fileId: ID of the extracted frame file as stored within Girder
    :type fileId: str
    :param sourceFileId: ID of the source file from which this extracted frame
                         is derived
     :type sourceFileId: str
    :param formatId: ID of the format that the source file had been transcoded
                     into before this frame had been extracted, or None if the
                     frame was extracted directly from the original source file.
    :type formatId: str
    :param index: Index of the frame in chronological order
    :type index: int
    """

    return {
        'fileId'          : fileId,
        'formatId'        : formatId,
        'sourceFileId'    : sourceFileId,
        'index'           : index
    }


def _createVideoJob(
    self, fileId=None, sourceFileId=None, formatId=None, jobId=None,
    jobType=None, **kwds):
    """
    Create a video entry for tracking a job launched by the video plugin.

    :param fileId: ID of the video file that is the subject of the tracked job
    :type fileId: str
    :param sourceFileId: ID of the source file from which the subject video file
                         is derived, or fileId if the subject is the original
                         source file.
    :type sourceFileId: str
    :param formatId: ID of the format that the tracked job is transcoding the
                     subject video file into, or None if the tracked job is not
                     a transcoding job.
    :type formatId: str
    :param jobId: ID of the job being tracked
    :type jobId: str
    :param jobType: Type of job being tracked
    :type jobType: str
    """

    return {
        'fileId'          : fileId,
        'formatId'        : formatId,
        'sourceFileId'    : sourceFileId,
        'jobId'           : jobId,
        'jobType'         : jobType
    }


class Video(Model):
    def initialize(self):
        self.name = 'video'
        self.ensureIndices([
            'fileId', 'index', 'jobId', 'sourceFileId', 'type'])

        self.exposeFields(level=AccessType.READ, fields={
            '_id', 'fileId', 'index', 'jobId', 'sourceFileId', 'type', 'name',
            'formatId', 'jobType', 'audioBitRate', 'audioChannelCount',
            'audioChannelLayout', 'audioCodec', 'audioSampleFormat',
            'audioSampleRate', 'duration', 'videoCodec', 'videoWidth',
            'videoFrameCount', 'videoHeight', 'videoBitRate', 'videoFrameRate'})

    def validate(self, data):
        # TODO(opadron): write the different validations
        #
        #  - type == FILE
        #  - type == FORMAT
        #  - type == FRAME
        #  - type == JOB
        return data

    def createVideo(self, **kwargs):
        return _handleCreateOperation(self, kwargs)

    def createVideoFile(self, **kwargs):
        return _handleCreateOperation(self, kwargs, _createVideoFile)

    def createVideoFormat(self, **kwargs):
        return _handleCreateOperation(self, kwargs, _createVideoFormat)

    def createVideoFrame(self, **kwargs):
        return _handleCreateOperation(self, kwargs, _createVideoFrame)

    def createVideoJob(self, **kwargs):
        return _handleCreateOperation(self, kwargs, _createVideoJob)

