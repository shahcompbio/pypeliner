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
        self.aliases = dict()
        self.rev_alias = collections.defaultdict(list)
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
        if (name, node) in self.aliases:
            return self.get_final_filename(*self.aliases[(name, node)])
        else:
            return os.path.join(self.temps_dir, nodes.name_node_filename(name, node))
    def add_alias(self, name, node, alias_name, alias_node):
        self.aliases[(alias_name, alias_node)] = (name, node)
        self.rev_alias[(name, node)].append((alias_name, alias_node))
    def get_aliases(self, name, node):
        for alias_name, alias_node in self.rev_alias[(name, node)]:
            yield (alias_name, alias_node)
            for alias_name_recurse, alias_node_recurse in self.get_aliases(alias_name, alias_node):
                yield (alias_name_recurse, alias_node_recurse)
    def is_temp_file(self, name, node):
        return str((name, node)) in self.createtimes_shelf
    def cleanup(self, depgraph):
        to_remove = set()
        for name, node in depgraph.obsolete:
            if (name, node) in self.aliases:
                continue
            if not self.is_temp_file(name, node):
                continue
            alias_ids = set([(name, node)] + list(self.get_aliases(name, node)))
            if alias_ids.issubset(depgraph.obsolete):
                filename = self.get_final_filename(name, node)
                if os.path.exists(filename):
                    os.remove(filename)
                to_remove.update(alias_ids)
        for name, node in to_remove:
            depgraph.obsolete.remove((name, node))

