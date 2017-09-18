import os
import pickle
import shutil
import logging

import pypeliner.helpers
import pypeliner.identifiers


class Dependency(object):
    """ An input/output in the dependency graph
    
    This class is mainly used for tracking dependencies in the
    dependency graph for which it is not appropriate to check
    timestamps, in particular for an axis chunk input to a job
    parallelized on that axis.
    """
    is_temp = False
    def __init__(self, name, node):
        self.name = name
        self.node = node
    @property
    def id(self):
        return (self.name, self.node)
    @property
    def exists(self):
        return True
    def build_displayname(self, base_node=pypeliner.identifiers.Node()):
        name = '/' + self.name
        if self.node.displayname != '':
            name = '/' + self.node.displayname + name
        if base_node.displayname != '':
            name = '/' + base_node.displayname + name
        return name
    def cleanup(self):
        pass


class Resource(Dependency):
    """ Abstract input/output in the dependency graph
    associated with a file tracked using creation time """
    def build_displayname_filename(self, base_node=pypeliner.identifiers.Node()):
        displayname = self.build_displayname(base_node)
        if displayname != self.filename:
            return ' '.join([displayname, self.filename])
        else:
            return displayname
    @property
    def exists(self):
        raise NotImplementedError
    @property
    def createtime(self):
        raise NotImplementedError
    def touch(self):
        raise NotImplementedError


class UserResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, storage, name, node, filename, direct_write=False):
        self.name = name
        self.node = node
        self.filename = filename
        self.store = storage.create_store(self.filename, is_temp=False, direct_write=direct_write)
    def build_displayname(self, base_node=pypeliner.identifiers.Node()):
        return self.filename
    @property
    def exists(self):
        return self.store.get_exists()
    @property
    def createtime(self):
        return self.store.get_createtime()
    def touch(self):
        if not self.exists:
            raise Exception('cannot touch missing user output')
        self.store.touch()
    def allocate_input(self):
        return self.store.allocate_input()
    def allocate_output(self):
        return self.store.allocate_output()
    def push(self):
        self.store.push()
    def pull(self):
        self.store.pull()


class TempFileResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, storage, name, node, filename, direct_write=False):
        self.name = name
        self.node = node
        self.filename = filename
        self.is_temp = True
        self.store = storage.create_store(self.filename, is_temp=True, direct_write=direct_write)
    @property
    def exists(self):
        return self.store.get_exists()
    @property
    def createtime(self):
        return self.store.get_createtime()
    def touch(self):
        self.store.touch()
    def cleanup(self):
        if self.exists:
            logging.getLogger('pypeliner.resources').debug('removing ' + self.filename)
            self.store.delete()
    def allocate_input(self):
        return self.store.allocate_input()
    def allocate_output(self):
        return self.store.allocate_output()
    def push(self):
        self.store.push()
    def pull(self):
        self.store.pull()


class TempObjResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, storage, name, node, filename, is_input=True):
        self.name = name
        self.node = node
        self.filename = filename + ('._i', '._o')[is_input]
        self.is_input = is_input
        self.is_temp = True
        self.store = storage.create_store(self.filename)
    @property
    def exists(self):
        return self.store.get_exists()
    @property
    def createtime(self):
        return self.store.get_createtime()
    def touch(self):
        if not self.exists:
            raise Exception('cannot touch missing object')
        self.store.touch()
    def get_obj(self):
        return self.store.get()
    def put_obj(self, obj):
        self.store.put(obj)


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
    def __init__(self, storage, name, node, filename):
        self.name = name
        self.node = node
        self.input = TempObjResource(storage, name, node, filename, is_input=True)
        self.output = TempObjResource(storage, name, node, filename, is_input=False)
    def get_obj(self):
        if self.input.exists:
            return self.input.get_obj()
    def finalize(self, obj):
        self.output.put_obj(obj)
        if not self.input.exists or not obj_equal(obj, self.get_obj()):
            self.input.put_obj(obj)
