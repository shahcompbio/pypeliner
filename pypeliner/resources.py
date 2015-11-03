import os
import pickle


class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename
    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)


class Dependency(object):
    """ An input/output in the dependency graph """
    def __init__(self, name, node):
        self.name = name
        self.node = node
    @property
    def id(self):
        return (self.name, self.node)


class Resource(Dependency):
    """ Abstract input/output in the dependency graph
    associated with a file tracked using creation time """
    @property
    def exists(self):
        raise NotImplementedError
    @property
    def createtime(self):
        raise NotImplementedError


class UserResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, name, node, fnames):
        self.name = name
        self.node = node
        fname_key = tuple([a[1] for a in node])
        if fnames is not None:
            if None in fname_key:
                self.filename = name
            else:
                if len(fname_key) == 1 and fname_key[0] in fnames:
                    self.filename = fnames[fname_key[0]]
                else:
                    self.filename = fnames[fname_key]
        else:
            self.filename = name.format(**dict(node))
    @property
    def exists(self):
        return os.path.exists(self.filename)
    @property
    def createtime(self):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
        return None
    @property
    def id(self):
        return (self.filename, ())
    @property
    def chunk(self):
        return self.node[-1][1]
    def finalize(self, write_filename):
        try:
            os.rename(write_filename, self.filename)
        except OSError:
            raise OutputMissingException(write_filename)


class TempFileResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, resmgr, name, node):
        self.resmgr = resmgr
        self.name = name
        self.node = node
        self.resmgr.register_disposable(name, node, self.filename)
    @property
    def filename(self):
        return self.resmgr.get_filename(self.name, self.node)
    @property
    def exists(self):
        return os.path.exists(self.filename)
    @property
    def createtime(self):
        return self.resmgr.retrieve_createtime(self.name, self.node, self.filename)
    @property
    def chunk(self):
        return self.node[-1][1]
    def finalize(self, write_filename):
        try:
            os.rename(write_filename, self.filename)
        except OSError:
            raise OutputMissingException(write_filename)
        self.resmgr.store_createtime(self.name, self.node, self.filename)


class TempObjResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, resmgr, name, node, is_input=True):
        self.resmgr = resmgr
        self.name = name
        self.node = node
        self.is_input = is_input
    @property
    def filename(self):
        return self.resmgr.get_filename(self.name, self.node) + ('._i', '._o')[self.is_input]
    @property
    def exists(self):
        return os.path.exists(self.filename)
    @property
    def createtime(self):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
        return None


def obj_equal(obj1, obj2):
    try:
        equal = obj1.__eq__(obj2)
        if equal is not NotImplemented:
            return equal
    except AttributeError:
        pass
    if obj1.__class__ != obj2.__class__:
        return False
    try:
        return obj1.__dict__ == obj2.__dict__
    except AttributeError:
        pass
    return obj1 == obj2


class TempObjManager(object):
    """ A file resource with filename and creation time if created """
    def __init__(self, resmgr, name, node):
        self.resmgr = resmgr
        self.name = name
        self.node = node
    @property
    def input(self):
        return TempObjResource(self.resmgr, self.name, self.node, is_input=True)
    @property
    def output(self):
        return TempObjResource(self.resmgr, self.name, self.node, is_input=False)
    @property
    def chunk(self):
        return self.node[-1][1]
    @property
    def obj(self):
        try:
            with open(self.input.filename, 'rb') as f:
                return pickle.load(f)
        except IOError as e:
            if e.errno == 2:
                pass
    def finalize(self, obj):
        with open(self.output.filename, 'wb') as f:
            pickle.dump(obj, f)
        if not obj_equal(obj, self.obj):
            with open(self.input.filename, 'wb') as f:
                pickle.dump(obj, f)


class ChunksResource(Resource):
    """ A resource representing a list of chunks for an axis """
    def __init__(self, nodemgr, axis, node):
        self.nodemgr = nodemgr
        self.axis = axis
        self.node = node
    @property
    def id(self):
        return (self.axis, self.node)
    @property
    def exists(self):
        return os.path.exists(self.nodemgr.get_chunks_filename(self.axis, self.node))
    @property
    def createtime(self):
        if self.exists:
            return os.path.getmtime(self.nodemgr.get_chunks_filename(self.axis, self.node))

