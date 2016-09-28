import os


_createtimes = dict()


def update_createtime(filename):
    if os.path.exists(filename):
        _createtimes[filename] = os.path.getmtime(filename)
    else:
        _createtimes[filename] = None


def get_exists(filename):
    if filename not in _createtimes:
        update_createtime(filename)
    return _createtimes[filename] is not None


def get_createtime(filename):
    if filename not in _createtimes:
        update_createtime(filename)
    return _createtimes[filename]


def invalidate_cached_state(filename):
    if filename in _createtimes:
        del _createtimes[filename]


def invalidate_all():
    global _createtimes
    _createtimes = dict()
