import os
import collections
import shelve

import resources
import nodes

class FilenameCreator(object):
    """ Function object for creating filenames from name node pairs """
    def __init__(self, file_dir='', file_suffix=''):
        self.file_dir = file_dir
        self.file_suffix = file_suffix
    def __call__(self, name, node):
        return os.path.join(self.file_dir, nodes.name_node_filename(name, node) + self.file_suffix)
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCreator.__module__, FilenameCreator.__name__, ', '.join(repr(a) for a in (self.file_dir, self.file_suffix)))

class ResourceManager(object):
    """ Manages file resources """
    def __init__(self, temps_dir, db_dir):
        self.temps_dir = temps_dir
        self.db_dir = db_dir
        self.temps_suffix = '.tmp'
    def __enter__(self):
        self.createtimes_shelf = shelve.open(os.path.join(self.db_dir, 'createtimes'))
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.createtimes_shelf.close()
    def get_input(self, name, node):
        final_filename = self.get_final_filename(name, node)
        return resources.TempResource(self, name, node, final_filename, final_filename)
    def get_output(self, name, node):
        temp_filename = self.get_temp_filename(name, node)
        final_filename = self.get_final_filename(name, node)
        return resources.TempResource(self, name, node, temp_filename, final_filename)
    def finalize_output(self, resource):
        final_filename = self.get_final_filename(resource.name, resource.node)
        try:
            os.rename(resource.filename, final_filename)
        except OSError:
            raise OutputMissingException(resource.filename)
        self.update_createtime(resource.name, resource.node, os.path.getmtime(final_filename))
    @property
    def filename_creator(self):
        return FilenameCreator(self.temps_dir, self.temps_suffix)
    def retrieve_createtime(self, name, node):
        final_filename = self.get_final_filename(name, node)
        if os.path.exists(final_filename):
            self.createtimes_shelf[str((name, node))] = os.path.getmtime(final_filename)
        return self.createtimes_shelf.get(str((name, node)), None)
    def update_createtime(self, name, node, createtime):
        self.createtimes_shelf[str((name, node))] = createtime
    def get_temp_filename(self, name, node):
        return self.get_final_filename(name, node) + self.temps_suffix
    def get_final_filename(self, name, node):
        return os.path.join(self.temps_dir, nodes.name_node_filename(name, node))
    def is_temp_file(self, name, node):
        return str((name, node)) in self.createtimes_shelf
    def cleanup(self, depgraph):
        for name, node in set(depgraph.obsolete):
            if self.is_temp_file(name, node):
                filename = self.get_final_filename(name, node)
                if os.path.exists(filename):
                    os.remove(filename)
            depgraph.obsolete.remove((name, node))

