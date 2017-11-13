#! /usr/bin/env python

import glob
import json
import os
import os.path
import subprocess
import sys

from re import compile

GIRDER_WORKER_DIR = os.environ.get('GWD', None)
if GIRDER_WORKER_DIR is None:
    GIRDER_WORKER_DIR = os.path.join('/', 'mnt', 'girder_worker', 'data')

RE_STREAM_BEGIN = compile(r'''^\[STREAM\]$''')
RE_STREAM_END = compile(r'''^\[/STREAM\]$''')
RE_FORMAT_BEGIN = compile(r'''^\[FORMAT\]$''')
RE_FORMAT_END = compile(r'''^\[/FORMAT\]$''')
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
    cmd = [FFPROBE, '-show_streams', '-show_format', input_file]
    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    proc = subprocess.Popen(
            cmd,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            universal_newlines=True)

    results = []
    format_info = None

    stream_info = None
    stream_index = None
    is_format = False
    for line in proc.stdout:
        line = line.strip()

        if RE_STREAM_BEGIN.match(line):
            stream_info = {}
            is_format = False
            continue

        if RE_STREAM_END.match(line):
            assert(not is_format)
            if stream_info is not None and stream_index is not None:
                results.extend((stream_index - len(results) + 1)*(None,))
                results[stream_index] = stream_info
                stream_info = None
                stream_index = None
                continue

        if RE_FORMAT_BEGIN.match(line):
            stream_info = {}
            is_format = True
            continue

        if RE_FORMAT_END.match(line):
            assert(is_format)
            format_info = stream_info
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

    return results, format_info


def analyze_main(args):
    input_file = next(glob.iglob(os.path.join(GIRDER_WORKER_DIR, 'input.*')))

    streams, format_info = probe(input_file)
    streams = streams or []
    format_info = format_info or {}

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

    audio_channel_count = convert(a_stream.get('channels'   ), int  , alt=None)
    audio_bit_rate      = convert(a_stream.get('bit_rate'   ), float, alt=None)
    audio_sample_rate   = convert(a_stream.get('sample_rate'), float, alt=None)

    audio_channel_layout = a_stream.get('channel_layout')
    audio_sample_format  = a_stream.get('sample_fmt'    )

    video_width      = convert(v_stream.get('width'    ), int  , alt=None)
    video_height     = convert(v_stream.get('height'   ), int  , alt=None)
    video_frame_count = convert(v_stream.get('nb_frames'), int  , alt=None)
    video_bit_rate    = convert(v_stream.get('bit_rate' ), float, alt=None)

    duration = convert(v_stream.get('duration'), float, alt=None)

    video_frame_rate = v_stream['avg_frame_rate']

    if '/' in video_frame_rate:
        a, b = tuple(float(x) for x in video_frame_rate.split('/', maxsplit=1))
        video_frame_rate = a/b
    else:
        video_frame_rate = convert(video_frame_rate, float, alt=None)

    if has_audio and audio_bit_rate is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-select_streams',
            'a:0', '-show_entries', 'stream=bit_rate', '-of',
            'default=nokey=1:noprint_wrappers=1']

        audio_bit_rate = subprocess.check_output(cmd, universal_newlines=True)
        audio_bit_rate = convert(audio_bit_rate.strip(), float, alt=None)

    if video_bit_rate is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-select_streams',
            'v:0', '-show_entries', 'stream=bit_rate', '-of',
            'default=nokey=1:noprint_wrappers=1']

        video_bit_rate = subprocess.check_output(cmd, universal_newlines=True)
        video_bit_rate = convert(video_bit_rate.strip(), float, alt=None)

    if video_frame_count is None:
        cmd = [
            FFPROBE, '-v', 'error', '-i', input_file, '-count_frames',
            '-select_streams', 'v:0', '-show_entries', 'stream=nb_read_frames',
            '-of', 'default=nokey=1:noprint_wrappers=1']

        video_frame_count = subprocess.check_output(cmd, universal_newlines=True)
        video_frame_count = convert(video_frame_count.strip(), int, alt=None)

    if format_info:
        start = convert(format_info.get('start_time'), float, alt=None)
        end = convert(format_info.get('duration'), float, alt=None)

        if start is not None and end is not None:
            other_duration = end - start

            if (duration is None or
                    (other_duration > 0 and
                     abs(duration - other_duration) >= 0.1)):
                duration = other_duration

        if video_frame_count is not None and duration is not None:
            other_frame_rate = float(video_frame_count)/float(duration)

            if (video_frame_rate is None or
                    (other_frame_rate > 0 and
                     abs(video_frame_rate - other_frame_rate) >= 0.01)):
                video_frame_rate = other_frame_rate

    result = {
        'audioBitRate'      : audio_bit_rate,
        'audioChannelCount' : audio_channel_count,
        'audioChannelLayout': audio_channel_layout,
        'audioSampleFormat' : audio_sample_format,
        'audioSampleRate'   : audio_sample_rate,
        'duration'          : duration,
        'videoBitRate'      : video_bit_rate,
        'videoFrameCount'   : video_frame_count,
        'videoFrameRate'    : video_frame_rate,
        'videoHeight'       : video_height,
        'videoWidth'        : video_width
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

    v_codec = None
    a_codec = None
    dimensions = None
    audio_sample_rate = None
    audio_bit_rate = None
    video_bit_rate = None

    for i in range(0, len(args) - 1, 2):
        code, value = args[i:i+2]
        if   code == 'VCODEC':
            v_codec = value
        elif code == 'ACODEC':
            a_codec = value
        elif code == 'DIMENSIONS':
            dimensions = value
        elif code == 'ASRATE':
            audio_sample_rate = value
        elif code == 'ABRATE':
            audio_bit_rate = value
        elif code == 'VBRATE':
            video_bit_rate = value

    file_extension = args[-1]

    cmd = [FFMPEG, '-i', input_file]
    if dimensions:
        # NOTE(opadron): There doesn't seem to be any difference between using
        #                -s and -vf, so I just use them both.
        cmd.extend(['-s', dimensions, '-vf', 'scale=%s' % dimensions])

    cmd.extend(['-quality', 'good', '-threads', '16'])

    if v_codec:
        cmd.extend([
            '-c:v', ('libvpx-vp9' if v_codec == 'VP9' else
                      v_codec)
        ])

    cmd.extend(['-crf', '5'])

    if video_bit_rate:
        cmd.extend(['-b:v', video_bit_rate])

    if a_codec:
        cmd.extend([
            '-c:a', ('libopus' if a_codec == 'OPUS' else
                     a_codec)
        ])

    if audio_sample_rate:
        cmd.extend(['-ar', audio_sample_rate])

    if audio_bit_rate:
        cmd.extend(['-b:a', audio_bit_rate])

    cmd.extend(['-g', '30', '-bf', '2'])
    cmd.append(os.path.join(GIRDER_WORKER_DIR, 'output.%s' % file_extension))

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

