
from bson.objectid import ObjectId

from io import TextIOBase

class DebugStream(TextIOBase):
    def __init__(self, delegate):
        self._delegate = delegate
        self._first_write = True
        self._opened = True

    def fileno(self, *args, **kwargs):
        return self._delegate.fileno(*args, **kwargs)

    def seek(self, *args, **kwargs):
        return self._delegate.seek(*args, **kwargs)

    def truncate(self, *args, **kwargs):
        return self._delegate.truncate(*args, **kwargs)

    def detach(self, *args, **kwargs):
        return self._delegate.detach(*args, **kwargs)

    def read(self, *args, **kwargs):
        return self._delegate.read(*args, **kwargs)

    def readline(self, *args, **kwargs):
        return self._delegate.readline(*args, **kwargs)

    def write(self, *args, **kwargs):
        if self._first_write:
            self._first_write = False
            self._delegate.write('\n')

        result = self._delegate.write(*args, **kwargs)
        self._delegate.flush()
        return result

    def closed(self, *args, **kwargs):
        return not self._opened

    def close(self, *args, **kwargs):
        if self._opened:
            self._opened = False
            if not self._first_write:
                self._delegate.write('\n')
                self._delegate.flush()



def objectIdOrNone(arg):
    return ObjectId(arg) if arg else None


def convertOrNone(x, func):
    try:
        x = func(x)
    except (ValueError, TypeError):
        x = None
    return x


def _debug(func, stream):
    from functools import wraps
    @wraps(func)
    def result(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            import sys, traceback
            traceback.print_exception(*sys.exc_info(), file=stream)
            stream.flush()
    return result


def debug(arg=None):
    func = None
    stream = None
    try:
        arg.__call__
        func = arg
    except:
        stream = arg

    if stream is None:
        import sys
        stream = sys.stdout

    if func is None:
        return lambda func: _debug(func, stream)

    return _debug(func, stream)


def _loud(func):
    from functools import wraps
    @wraps(func)
    def result(*args, **kwargs):
        import sys
        _old_stdout = sys.stdout
        sys.stdout = DebugStream(sys.stdout)

        try:
            return func(*args, **kwargs)
        finally:
            sys.stdout.close()
            sys.stdout = _old_stdout
    return result


def loud(arg=None):
    func = None
    try:
        arg.__call__
        func = arg
    except:
        pass

    if func is None:
        return lambda func: _loud(func)

    return _loud(func)


def floatOrNone(x):
    return convertOrNone(x, float)


def intOrNone(x):
    return convertOrNone(x, int)


