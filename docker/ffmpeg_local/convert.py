#! /usr/bin/env python

import glob
import json
import os.path
import subprocess
import sys

from re import compile

GIRDER_WORKER_DIR = os.path.join('/', 'mnt', 'girder_worker', 'data')

RE_STREAM_BEGIN = compile(r'''^\[STREAM\]$''')
RE_STREAM_END = compile(r'''^\[/STREAM\]$''')
RE_PROGRESS_INFO = compile(r'''^frame=([ 0-9]+)''')

FFMPEG = 'ffmpeg'
FFPROBE = 'ffprobe'

def convert(x, type=int, **kwds):
    try:
        return type(x)
    except (ValueError, TypeError):
        return kwds.get('alt', x)


def duration_parse(durstr):
    """Parse a duration of the form [[hh:]mm:]ss[.sss] and return a float
     with the duration or None for failure to parse.
    Enter: durstr: string with the duration.
    Exit:  duration: duration in seconds."""
    try:
        durstr = durstr.strip().split(":")
        dur = 0
        for part in range(len(durstr)):
            dur += float(durstr[-1-part])*(60**part)
    except Exception:
        return None
    return dur


def check_exit_code(code, cmd):
    if code:
        raise RuntimeError('command {} returned exit code {}'.format(
            repr(cmd), code))


def probe(input_file):
    cmd = [FFPROBE, '-show_streams', input_file]
    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    proc = subprocess.Popen(
            cmd,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            universal_newlines=True)

    results = []

    stream_info = None
    stream_index = None
    for line in proc.stdout:
        line = line.strip()

        if RE_STREAM_BEGIN.match(line):
            stream_info = {}
            continue

        if RE_STREAM_END.match(line):
            if stream_info is not None and stream_index is not None:
                results.extend((stream_index - len(results) + 1)*(None,))
                results[stream_index] = stream_info
                stream_info = None
                stream_index = None
                continue

        if stream_index is None and line.startswith('index='):
            stream_index = int(line.split('=')[1])
            continue

        if stream_info is not None:
            k, v = line.split('=', maxsplit=1)
            stream_info[k] = v
            continue

    proc.stdout.close()
    check_exit_code([proc.wait(), 0][1], cmd)

    return results


def analyze_main(args):
    input_file = next(glob.iglob(os.path.join(GIRDER_WORKER_DIR, 'input.*')))

    streams = probe(input_file)

    a_stream = None
    v_stream = None

    for s in streams:
        if a_stream is None and s['codec_type'] == 'audio':
            a_stream = s

        if v_stream is None and s['codec_type'] == 'video':
            v_stream = s

        if a_stream is not None and v_stream is not None:
            break

    assert(v_stream)
    has_audio = bool(a_stream)
    a_stream = a_stream or {}

    audioChannelCount = convert(a_stream.get('channels'   ), int  , alt=None)
    audioBitRate      = convert(a_stream.get('bit_rate'   ), float, alt=None)
    audioSampleRate   = convert(a_stream.get('sample_rate'), float, alt=None)

    audioChannelLayout = a_stream.get('channel_layout')
    audioSampleFormat  = a_stream.get('sample_fmt'    )

    videoWidth      = convert(v_stream.get('width'    ), int  , alt=None)
    videoHeight     = convert(v_stream.get('height'   ), int  , alt=None)
    videoFrameCount = convert(v_stream.get('nb_frames'), int  , alt=None)
    videoBitRate    = convert(v_stream.get('bit_rate' ), float, alt=None)

    duration = convert(v_stream.get('duration'), float, alt=None)

    videoFrameRate = v_stream['avg_frame_rate']

    if '/' in videoFrameRate:
        a, b = tuple(float(x) for x in videoFrameRate.split('/', maxsplit=1))
        videoFrameRate = a/b
    else:
        videoFrameRate = convert(videoFrameRate, float, alt=None)

    if has_audio and audioBitRate is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-select_streams',
            'a:0', '-show_entries', 'stream=bit_rate', '-of',
            'default=nokey=1:noprint_wrappers=1']

        audioBitRate = subprocess.check_output(cmd, universal_newlines=True)
        audioBitRate = convert(audioBitRate.strip(), float, alt=None)

    if videoBitRate is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-select_streams',
            'v:0', '-show_entries', 'stream=bit_rate', '-of',
            'default=nokey=1:noprint_wrappers=1']

        videoBitRate = subprocess.check_output(cmd, universal_newlines=True)
        videoBitRate = convert(videoBitRate.strip(), float, alt=None)

    if videoFrameCount is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-count_frames',
            '-select_streams', 'v:0', '-show_entries', 'stream=nb_read_frames',
            '-of', 'default=nokey=1:noprint_wrappers=1']

        videoFrameCount = subprocess.check_output(cmd, universal_newlines=True)
        videoFrameCount = convert(videoFrameCount.strip(), int, alt=None)

    if duration is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-show_entries',
            'format=duration', '-of', 'default=nokey=1:noprint_wrappers=1']

        duration = subprocess.check_output(cmd, universal_newlines=True)
        duration = convert(duration.strip(), float, alt=None)

    result = {
        'audioBitRate'      : audioBitRate,
        'audioChannelCount' : audioChannelCount,
        'audioChannelLayout': audioChannelLayout,
        'audioSampleFormat' : audioSampleFormat,
        'audioSampleRate'   : audioSampleRate,
        'duration'          : duration,
        'videoBitRate'      : videoBitRate,
        'videoFrameCount'   : videoFrameCount,
        'videoFrameRate'    : videoFrameRate,
        'videoHeight'       : videoHeight,
        'videoWidth'        : videoWidth
    }

    with open(os.path.join(GIRDER_WORKER_DIR, 'meta.json'), 'w') as f:
        json.dump(result, f, indent=2)


def extract_main(args):
    num_frames = int(args[0])
    input_file = next(glob.iglob(os.path.join(GIRDER_WORKER_DIR, 'input.*')))

    cmd = [
        FFMPEG, '-i', input_file,
        os.path.join(GIRDER_WORKER_DIR, '%d.png')
    ]

    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    subprocess.check_call(cmd)

def transcode_main(args):
    input_file = next(glob.iglob(os.path.join(GIRDER_WORKER_DIR, 'input.*')))

    vCodec = None
    aCodec = None
    dimensions = None
    asRate = None
    abRate = None
    vbRate = None

    for i in range(0, len(args) - 1, 2):
        code, value = args[i:i+2]
        if   code == 'VCODEC':
            vCodec = value
        elif code == 'ACODEC':
            aCodec = value
        elif code == 'DIMENSIONS':
            dimensions = value
        elif code == 'ASRATE':
            asRate = value
        elif code == 'ABRATE':
            abRate = value
        elif code == 'VBRATE':
            vbRate = value

    fileExtension = args[-1]

    cmd = [FFMPEG, '-i', input_file]
    if dimensions:
        # NOTE(opadron): There doesn't seem to be any difference between using
        #                -s and -vf, so I just use them both.
        cmd.extend(['-s', dimensions, '-vf', 'scale=%s' % dimensions])

    cmd.extend(['-quality', 'good', '-threads', '16'])

    if vCodec:
        cmd.extend([
            '-c:v', ('libvpx-vp9' if vCodec == 'VP9' else
                      vCodec)
        ])

    cmd.extend(['-crf', '5'])

    if vbRate:
        cmd.extend(['-b:v', vbRate])

    if aCodec:
        cmd.extend([
            '-c:a', ('libopus' if aCodec == 'OPUS' else
                     aCodec)
        ])

    if asRate:
        cmd.extend(['-ar', asRate])

    if abRate:
        cmd.extend(['-b:a', abRate])

    cmd.extend(['-g', '30', '-bf', '2'])
    cmd.append(os.path.join(GIRDER_WORKER_DIR, 'output.%s' % fileExtension))

    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    subprocess.check_call(cmd)

def main():
    mode = sys.argv[1]
    args = sys.argv[2:]

    if mode == 'analyze':
        analyze_main(args)

    if mode == 'extract':
        extract_main(args)

    if mode == 'transcode':
        transcode_main(args)

if __name__ == '__main__':
    try:
        main()
    except:
        import sys, traceback
        traceback.print_exc(file=sys.stdout)
        traceback.print_stack(file=sys.stdout)
        sys.stdout.flush()
        raise

