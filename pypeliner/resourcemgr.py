import os
import collections
import shelve
import pickle

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
    def create_input_file_resource(self, name, node):
        """ Create a Resource representing a temporary file """
        filename = self.get_final_filename(name, node)
        createtime = self.retrieve_createtime(name, node, filename)
        return resources.TempResource(self, name, node, filename, createtime)
    def create_output_file_resource(self, name, node):
        """ Create a Resource representing a temporary file """
        filename = self.get_final_filename(name, node)
        temp_filename = self.get_temp_filename(name, node)
        createtime = self.retrieve_createtime(name, node, filename)
        return resources.TempResource(self, name, node, temp_filename, createtime)
    def create_input_object_resource(self, name, node):
        """ Create a Resource representing a temporary object to be used as an input """
        filename = self.get_input_object_filename(name, node)
        createtime = self.retrieve_createtime(name, node, filename)
        return resources.TempResource(self, name, node, filename, createtime)
    def create_output_object_resource(self, name, node):
        """ Create a Resource representing a temporary object to be used as an output """
        filename = self.get_output_object_filename(name, node)
        createtime = self.retrieve_createtime(name, node, filename)
        return resources.TempResource(self, name, node, filename, createtime)
    def finalize_output(self, resource):
        """ Finalize a temporary file written to by a job """
        final_filename = self.get_final_filename(resource.name, resource.node)
        final_filename = self.get_final_filename(resource.name, resource.node)
        try:
            os.rename(resource.filename, final_filename)
        except OSError:
            raise OutputMissingException(resource.filename)
        self.update_createtime(resource.name, resource.node, os.path.getmtime(final_filename))
    @property
    def filename_creator(self):
        return FilenameCreator(self.temps_dir, self.temps_suffix)
    def retrieve_createtime(self, name, node, filename):
        if os.path.exists(filename):
            self.createtimes_shelf[str((name, node))] = os.path.getmtime(filename)
        return self.createtimes_shelf.get(str((name, node)), None)
    def update_createtime(self, name, node, createtime):
        self.createtimes_shelf[str((name, node))] = createtime
    def get_temp_filename(self, name, node):
        return self.get_final_filename(name, node) + self.temps_suffix
    def get_final_filename(self, name, node):
        return os.path.join(self.temps_dir, nodes.name_node_filename(name, node))
    def get_input_object_filename(self, name, node):
        """ Get the filename to read an object for jobs using the object as an input """
        return os.path.join(self.temps_dir, nodes.name_node_filename(name, node)) + '._i'
    def get_output_object_filename(self, name, node):
        """ Get the filename to write an object for jobs using the object as an output """
        return os.path.join(self.temps_dir, nodes.name_node_filename(name, node)) + '._o'
    def finalize_object(self, name, node, obj):
        """ Finalize an object after it has been created by a job

        Write the object to its filename as an output.  If the input version of the object exists, read that and check 
        if the output version is different.  If the output version is different, overwrite the input version with the
        new object.

        """
        input_filename = self.get_input_object_filename(name, node)
        output_filename = self.get_output_object_filename(name, node)
        with open(output_filename, 'wb') as f:
            pickle.dump(obj, f)
        prev_obj = self.retrieve_object(name, node)
        if prev_obj is None or not obj == prev_obj:
            with open(input_filename, 'wb') as f:
                pickle.dump(obj, f)
    def retrieve_object(self, name, node):
        input_filename = self.get_input_object_filename(name, node)
        try:
            with open(input_filename, 'rb') as f:
                return pickle.load(f)
        except IOError as e:
            if e.errno == 2:
                pass
    def is_temp_file(self, name, node):
        return str((name, node)) in self.createtimes_shelf
    def cleanup(self, depgraph):
        for name, node in set(depgraph.obsolete):
            if self.is_temp_file(name, node):
                filename = self.get_final_filename(name, node)
                if os.path.exists(filename):
                    os.remove(filename)
            depgraph.obsolete.remove((name, node))

