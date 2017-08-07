#! /usr/bin/env python

import glob
import json
import os.path
import subprocess
import sys
import traceback

from re import compile

GIRDER_WORKER_DIR = os.path.join('/', 'mnt', 'girder_worker', 'data')

RE_VIDEO_INFO = compile(r'''^\s+Stream #.+?: Video''')
RE_AUDIO_INFO = compile(r'''^\s+Stream #.+?: Audio''')
RE_DURATION_INFO = compile(r'''^\s+Duration: ''')

FFMPEG = 'ffmpeg'

def duration_parse(durstr):
    """Parse a duration of the form [[hh:]mm:]ss[.sss] and return a float
     with the duration or None for failure to parse.
    Enter: durstr: string with the duration.
    Exit:  duration: duration in seconds."""
    try:
        durstr = durstr.strip().split(":")
        dur = 0
        for part in xrange(len(durstr)):
            dur += float(durstr[-1-part])*(60**part)
    except Exception:
        return None
    return dur

subprocess.call(['ls', GIRDER_WORKER_DIR])
sys.stdout.flush()

input_file = next(glob.iglob(os.path.join(GIRDER_WORKER_DIR, 'input.*')))

cmd = [FFMPEG, '-i', input_file]
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

            elif ' fps' in part and not meta['video'].get('fps'):
                meta['video']['fps'] = float(part.split(' fps')[0].strip())

            elif ' kb/s' in part and not meta['video'].get('bitRate'):
                meta['video']['bitRate'] = float(part.split(' kb/s')[0].strip())

    if RE_AUDIO_INFO.match(line) and not meta['audio'].get('bitRate'):
        for part in line.split(',')[1:]:
            if ' kb/s' in part and not meta['audio'].get('bitRate'):
                meta['audio']['bitRate'] = float(part.split(' kb/s')[0].strip())

            elif ' Hz' in part and not meta['video'].get('sampleRate'):
                meta['video']['sampleRate'] = (
                        float(part.split(' Hz')[0].strip()))

    if RE_DURATION_INFO.match(line) and not meta.get('duration'):
        meta['duration'] = duration_parse(
                line.split("Duration:", 1)[1].split(",")[0])

proc.stderr.close()
proc.wait()

cmd.extend(['-vcodec', 'copy', '-an', '-f', 'null', 'null'])
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

if calcdur:
    meta['duration'] = calcdur
    meta['video']['frameCount'] = calcframe
    meta['video']['frameRate'] = float(calcframe)/calcdur

proc.stderr.close()
proc.wait()

# TODO(opadron): work out multiple quality versions (-crf 30).
cmd = [FFMPEG, '-i', input_file, '-vf', 'scale=640x480', '-quality', 'good',
        '-speed', '0', '-c:v', 'libvpx-vp9', '-crf', '5', '-b:v', '100k', '-c:a',
        'libopus', os.path.join(GIRDER_WORKER_DIR, 'source.webm')]
subprocess.check_call(cmd)

with open(os.path.join(GIRDER_WORKER_DIR, 'meta.json'), 'w') as f:
    json.dump(meta, f, indent=2)

