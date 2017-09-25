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


def main():
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

    if calcdur:
        meta['duration'] = calcdur
        meta['video']['frameCount'] = calcframe
        meta['video']['frameRate'] = float(calcframe)/calcdur

    proc.stderr.close()
    check_exit_code([proc.wait(), 0][1], cmd)

    # TODO(opadron): work out multiple quality versions (-crf 30).
    cmd = [
        FFMPEG, '-i', input_file, '-vf', 'scale=640x480', '-quality', 'good',
        '-speed', '0', '-c:v', 'libvpx-vp9', '-crf', '5', '-b:v', '100k',
        '-c:a', 'libopus', os.path.join(GIRDER_WORKER_DIR, 'source.webm')]

    sys.stdout.write(' '.join(('RUN:', repr(cmd))))
    sys.stdout.write('\n')
    sys.stdout.flush()

    meta_dump = json.dumps(meta, indent=2)
    with open(os.path.join(GIRDER_WORKER_DIR, '.girder_progress'), 'w') as prog:
        proc = subprocess.Popen(
                cmd,
                stderr=subprocess.PIPE,
                universal_newlines=True)

        total = int(calcframe + len(meta_dump))
        for line in proc.stderr:
            if calcdur:
                m = RE_PROGRESS_INFO.match(line)
                if m:
                    progress_update = {
                        'message': 'transcoding video...',
                        'total': total,
                        'current': int(m.group(1).strip())
                    }

                    json.dump(progress_update, prog)
                    prog.flush()

            sys.stderr.write(line)
            sys.stderr.flush()

        proc.stderr.close()
        check_exit_code([proc.wait(), 0][1], cmd)

        progress_update = {
            'message': 'writing metadata...',
            'total': total,
            'current': total
        }

        json.dump(progress_update, prog)
        prog.flush()

        with open(os.path.join(GIRDER_WORKER_DIR, 'meta.json'), 'w') as f:
            f.write(meta_dump)

if __name__ == '__main__':
    try:
        main()
    finally:
        for fpath in (os.path.join(GIRDER_WORKER_DIR, 'meta.json'),
                      os.path.join(GIRDER_WORKER_DIR, 'source.webm'),):

            if os.path.exists(fpath):
                continue

            with open(fpath, 'w') as f:
                pass # touch

