#! /usr/bin/env python

import os.path
import subprocess
import sys
import traceback

try:
    subprocess.check_call([
        'ls', '-la', os.path.join('/', 'mnt', 'girder_worker', 'data')])

    sys.stdout.write('Hello, stdout!\n')
    sys.stderr.write('Hello, stderr!\n')
except Exception:
    traceback.print_exception(*sys.exc_info())

