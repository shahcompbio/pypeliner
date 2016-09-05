import os
import pickle
import shutil

import pypeliner.helpers
import pypeliner.identifiers


class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename
    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)


class Dependency(object):
    """ An input/output in the dependency graph
    
    This class is mainly used for tracking dependencies in the
    dependency graph for which it is not appropriate to check
    timestamps, in particular for an axis chunk input to a job
    parallelized on that axis.
    """
    def __init__(self, db, name, node, **kwargs):
        self.name = name
        self.node = node
    @property
    def id(self):
        return (self.name, self.node)
    def get_exists(self, db):
        return True
    def build_displayname(self, base_node=pypeliner.identifiers.Node()):
        name = '/' + self.name
        if self.node.displayname != '':
            name = '/' + self.node.displayname + name
        if base_node.displayname != '':
            name = '/' + base_node.displayname + name
        return name


class Resource(Dependency):
    """ Abstract input/output in the dependency graph
    associated with a file tracked using creation time """
    def build_displayname_filename(self, db, base_node=pypeliner.identifiers.Node()):
        displayname = self.build_displayname(base_node)
        filename = self.get_filename(db)
        if displayname != filename:
            return ' '.join([displayname, filename])
        else:
            return displayname
    def get_filename(self, db):
        raise NotImplementedError
    def get_exists(self, db):
        raise NotImplementedError
    def get_createtime(self, db):
        raise NotImplementedError
    def touch(self, db):
        raise NotImplementedError


def resolve_user_filename(name, node, fnames=None, template=None):
    """ Resolve a filename based on user provided information """
    fname_key = tuple([a[1] for a in node])
    if fnames is not None:
        if len(fname_key) == 1:
            filename = fnames.get(fname_key[0], name)
        else:
            filename = fnames.get(fname_key, name)
    elif template is not None:
        filename = template.format(**dict(node))
    else:
        filename = name.format(**dict(node))
    return filename


class UserResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, db, name, node, fnames=None, template=None, **kwargs):
        self.name = name
        self.node = node
        self.filename = resolve_user_filename(name, node, fnames=fnames, template=template)
    def get_filename(self, db):
        return self.filename
    def build_displayname(self, base_node=pypeliner.identifiers.Node()):
        return self.filename
    def get_exists(self, db):
        return os.path.exists(self.filename)
    def get_createtime(self, db):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
        return None
    def touch(self, db):
        if not self.get_exists(db):
            raise Exception('cannot touch missing user output')
        pypeliner.helpers.touch(self.filename)
    def finalize(self, write_filename, db):
        try:
            os.rename(write_filename, self.filename)
        except OSError:
            raise OutputMissingException(write_filename)


def get_temp_filename(temps_dir, name, node):
    return os.path.join(temps_dir, node.subdir, name)


class TempFileResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, db, name, node, temps_dir=None, **kwargs):
        self.name = name
        self.node = node
        self.temps_dir = temps_dir
        db.resmgr.register_disposable(self.name, self.node, self.get_filename(db))
    def get_filename(self, db):
        return get_temp_filename(self.temps_dir, self.name, self.node)
    def _get_createtime_placeholder(self, db):
        return self.get_filename(db) + '._placeholder'
    def _save_createtime(self, db):
        placeholder_filename = self._get_createtime_placeholder(db)
        pypeliner.helpers.saferemove(placeholder_filename)
        pypeliner.helpers.touch(placeholder_filename)
        shutil.copystat(self.get_filename(db), placeholder_filename)
    def get_exists(self, db):
        return os.path.exists(self.get_filename(db))
    def get_createtime(self, db):
        if os.path.exists(self.get_filename(db)):
            return os.path.getmtime(self.get_filename(db))
        placeholder_filename = self._get_createtime_placeholder(db)
        if os.path.exists(placeholder_filename):
            return os.path.getmtime(placeholder_filename)
    def touch(self, db):
        if self.get_exists(db):
            pypeliner.helpers.touch(self.get_filename(db))
            self._save_createtime(db)
        else:
            placeholder_filename = self._get_createtime_placeholder(db)
            pypeliner.helpers.touch(placeholder_filename)
    def finalize(self, write_filename, db):
        try:
            os.rename(write_filename, self.get_filename(db))
        except OSError:
            raise OutputMissingException(write_filename)
        self._save_createtime(db)


class TempObjResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, name, node, is_input=True, temps_dir=None, **kwargs):
        self.name = name
        self.node = node
        self.is_input = is_input
        self.temps_dir = temps_dir
    def get_filename(self, db):
        return get_temp_filename(self.temps_dir, self.name, self.node) + ('._i', '._o')[self.is_input]
    def get_exists(self, db):
        return os.path.exists(self.get_filename(db))
    def get_createtime(self, db):
        if os.path.exists(self.get_filename(db)):
            return os.path.getmtime(self.get_filename(db))
        return None
    def touch(self, db):
        if not self.get_exists(db):
            raise Exception('cannot touch missing user output')
        pypeliner.helpers.touch(self.get_filename(db))


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
    def __init__(self, db, name, node, temps_dir=None, **kwargs):
        self.name = name
        self.node = node
        self.temps_dir = temps_dir
    @property
    def input(self):
        return TempObjResource(self.name, self.node, is_input=True, temps_dir=self.temps_dir)
    @property
    def output(self):
        return TempObjResource(self.name, self.node, is_input=False, temps_dir=self.temps_dir)
    def get_obj(self, db):
        try:
            with open(self.input.get_filename(db), 'rb') as f:
                return pickle.load(f)
        except IOError as e:
            if e.errno == 2:
                pass
    def finalize(self, obj, db):
        with open(self.output.get_filename(db), 'wb') as f:
            pickle.dump(obj, f)
        if not obj_equal(obj, self.get_obj(db)):
            with open(self.input.get_filename(db), 'wb') as f:
                pickle.dump(obj, f)

