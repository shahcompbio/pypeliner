import os
import logging
import stat
import hashlib

def abspath(path):
    if path.endswith('/'):
        return os.path.abspath(path) + '/'
    else:
        return os.path.abspath(path)

class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        header = logging.Formatter.format(self, record)
        return header + record.message.rstrip('\n').replace('\n', '\n\t')

def which(name):
    if os.environ.get('PATH', None) is not None:
        for p in os.environ.get('PATH', '').split(os.pathsep):
            p = os.path.join(p, name)
            if os.access(p, os.X_OK):
                return p
    raise Exception('unable to find ' + name + ' in the system path')

def set_executable(filename):
    mode = os.stat(filename).st_mode
    mode |= stat.S_IXUSR
    os.chmod(filename, stat.S_IMODE(mode))

def md5_file(filename, block_size=8192):
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(block_size), b''): 
             md5.update(chunk)
    return md5.digest()

def overwrite_if_different(new_filename, existing_filename):
    do_copy = True
    try:
        do_copy = md5_file(existing_filename) != md5_file(new_filename)
    except IOError:
        pass
    if do_copy:
        os.rename(new_filename, existing_filename)

def makedirs(dirname):
    try:
        os.makedirs(dirname)
    except OSError as e:
        if e.errno != 17:
            raise
    assert os.path.isdir(dirname)

def symlink(source, link_name):
    source = os.path.abspath(source)
    try:
        os.remove(link_name)
    except OSError as e:
        if e.errno != 2:
            raise
    os.symlink(source, link_name)

class SubmitException(Exception):
    pass

class PipelineException(Exception):
    pass

