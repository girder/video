#! /usr/bin/env python

import glob
import json
import os.path
import subprocess
import sys

from re import compile

GIRDER_WORKER_DIR = os.path.join('/', 'mnt', 'girder_worker', 'data')

RE_VIDEO_INFO = compile(r'''^\s+Stream #.+?: Video''')
RE_AUDIO_INFO = compile(r'''^\s+Stream #.+?: Audio''')
RE_DURATION_INFO = compile(r'''^\s+Duration: ''')
RE_PROGRESS_INFO = compile(r'''^frame=([ 0-9]+)''')

FFMPEG = 'ffmpeg'


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


def analyze_main(args):
    input_file = next(glob.iglob(os.path.join(GIRDER_WORKER_DIR, 'input.*')))

    cmd = [FFMPEG, '-i', input_file]
    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    proc = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            universal_newlines=True)

    meta = {'audio': {}, 'video': {}}
    for line in proc.stderr:
        if RE_VIDEO_INFO.match(line) and not meta['video'].get('width'):
            for part in line.split(',')[1:]:
                if 'x' in part and not meta['video'].get('width'):
                    meta['video']['width'] = int(
                            part.split('x')[0].split()[-1].strip())

                    meta['video']['height'] = int(
                            part.strip().split('x')[1].split()[0].strip())

                elif ' fps' in part and not meta['video'].get('frameRate'):
                    meta['video']['frameRate'] = float(
                            part.split(' fps')[0].strip())

                elif ' kb/s' in part and not meta['video'].get('bitRate'):
                    meta['video']['bitRate'] = float(
                            part.split(' kb/s')[0].strip())

        if RE_AUDIO_INFO.match(line) and not meta['audio'].get('bitRate'):
            for part in line.split(',')[1:]:
                if ' kb/s' in part and not meta['audio'].get('bitRate'):
                    meta['audio']['bitRate'] = float(
                            part.split(' kb/s')[0].strip())

                elif ' Hz' in part and not meta['audio'].get('sampleRate'):
                    meta['audio']['sampleRate'] = (
                            float(part.split(' Hz')[0].strip()))

        if RE_DURATION_INFO.match(line) and not meta.get('duration'):
            meta['duration'] = duration_parse(
                    line.split("Duration:", 1)[1].split(",")[0])

    proc.stderr.close()
    check_exit_code([proc.wait(), 0][1], cmd)

    cmd.extend(['-vcodec', 'copy', '-an', '-f', 'null', 'null'])
    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    proc = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            universal_newlines=True)

    calcdur = None
    calcframe = None

    for line in proc.stderr:
        if line.startswith('frame=') and ' time=' in line:
            calcframe = int(line.split("frame=")[1].split()[0])
            calcdur = duration_parse(line.split(" time=")[1].split()[0])

    fps = 1
    if calcdur:
        meta['duration'] = calcdur
        meta['video']['frameCount'] = calcframe

        fps = float(calcframe)/calcdur
        meta['video']['frameRate'] = fps
        fps = int(fps + 0.5)

    proc.stderr.close()
    check_exit_code([proc.wait(), 0][1], cmd)

    meta_dump = json.dumps(meta, indent=2)
    with open(os.path.join(GIRDER_WORKER_DIR, 'meta.json'), 'w') as f:
        f.write(meta_dump)


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

    cmd.append(os.path.join(GIRDER_WORKER_DIR, 'output.%s' % fileExtension))

    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    subprocess.check_call(cmd)

if __name__ == '__main__':
    mode = sys.argv[1]
    args = sys.argv[2:]

    if mode == 'analyze':
        analyze_main(args)

    if mode == 'extract':
        extract_main(args)

    if mode == 'transcode':
        transcode_main(args)

