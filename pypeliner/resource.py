import os

class Dependency(object):
    """ An input/output in the dependency graph """
    def __init__(self, axis, node):
        self.axis = axis
        self.node = node
    @property
    def id(self):
        return (self.axis, self.node)

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
    def __init__(self, filename):
        self.filename = filename
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

class TempResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, resmgr, name, node, temp_filename, final_filename):
        self.resmgr = resmgr
        self.name = name
        self.node = node
        self.temp_filename = temp_filename
        self.final_filename = final_filename
    @property
    def filename(self):
        return self.temp_filename
    @property
    def exists(self):
        return os.path.exists(self.final_filename)
    @property
    def createtime(self):
        return self.resmgr.retrieve_createtime(self.name, self.node)
    @property
    def id(self):
        return (self.name, self.node)

