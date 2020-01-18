import logging

import pypeliner.identifiers
import dill as pickle


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

    def allocate(self):
        self.store.allocate()
        for store in self.extra_stores:
            store.allocate()

    def push(self):
        self.store.push()
        for store in self.extra_stores:
            store.push()

    def pull(self):
        self.store.pull()
        for store in self.extra_stores:
            store.pull()


class UserResource(Resource):
    """ A file resource with filename and creation time if created """

    def __init__(self, storage, name, node, filename, direct_write=False, extensions=None, store_dir=None):
        self.name = name
        self.node = node
        # Note:
        # filenames may be none if they are not yet defined
        # for example for a job that creates axis chunks that
        # are used by the same job's outputs
        self.extra_stores = []
        if filename is None:
            self.store = None
            self.filename = None
        else:
            self.store = storage.create_store(
                filename, is_temp=False, direct_write=direct_write, store_dir=store_dir
            )
            self.filename = self.store.filename
            if extensions is not None:
                for ext in extensions:
                    self.extra_stores.append(
                        storage.create_store(
                            filename, extension=ext, is_temp=False, direct_write=direct_write,
                            store_dir=store_dir
                        )
                    )

    def build_displayname(self, base_node=pypeliner.identifiers.Node()):
        return self.filename

    @property
    def exists(self):
        if self.store is None:
            return None
        return self.store.get_exists()

    @property
    def createtime(self):
        if self.store is None:
            return None
        return self.store.get_createtime()

    @property
    def write_filename(self):
        return self.store.write_filename

    def touch(self):
        if not self.exists:
            raise Exception('cannot touch missing user output')
        self.store.touch()


class TempFileResource(Resource):
    """ A file resource with filename and creation time if created """

    def __init__(self, storage, name, node, filename, direct_write=False, extensions=None, store_dir=None):
        self.name = name
        self.node = node
        self.is_temp = True
        self.store = storage.create_store(
            filename, is_temp=True, direct_write=direct_write, store_dir=store_dir
        )
        self.filename = self.store.filename
        self.extra_stores = []
        if extensions is not None:
            for ext in extensions:
                self.extra_stores.append(
                    storage.create_store(
                        filename, extension=ext, is_temp=False, direct_write=direct_write,
                        store_dir=store_dir
                    )
                )

    @property
    def exists(self):
        return self.store.get_exists()

    @property
    def createtime(self):
        return self.store.get_createtime()

    @property
    def write_filename(self):
        return self.store.write_filename

    def touch(self):
        self.store.touch()

    def cleanup(self):
        if self.exists:
            logging.getLogger('pypeliner.resources').debug('removing ' + self.filename)
            self.store.delete()


class TempObjResource(Resource):
    """ A file resource with filename and creation time if created """

    def __init__(self, storage, name, node, filename, is_input=True):
        self.name = name
        self.node = node
        self.filename = filename + ('._i', '._o')[is_input]
        self.is_input = is_input
        self.is_temp = True
        self.store = storage.create_store(self.filename, is_temp=True)

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
        self.store.allocate()
        self.store.pull()

        with open(self.store.filename, 'rb') as f:
            return pickle.load(f)

    def put_obj(self, obj):
        self.store.allocate()
        with open(self.store.write_filename, 'wb') as f:
            pickle.dump(obj, f)
        self.store.push()


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
            try:
                return self.input.get_obj()
            except:
                return

    def finalize(self, obj):
        self.output.put_obj(obj)

        existing_obj = self.get_obj()

        if existing_obj is None or not obj_equal(obj, existing_obj):
            self.input.put_obj(obj)
