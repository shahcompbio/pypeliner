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
        self.disposable = collections.defaultdict(set)
        self.aliases = dict()
        self.rev_alias = collections.defaultdict(list)
    def __enter__(self):
        self.createtimes_shelf = shelve.open(os.path.join(self.db_dir, 'createtimes'))
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.createtimes_shelf.close()
    @property
    def filename_creator(self):
        return FilenameCreator(self.temps_dir, self.temps_suffix)
    def retrieve_createtime(self, name, node, filename):
        if os.path.exists(filename):
            self.createtimes_shelf[str((name, node))] = os.path.getmtime(filename)
        return self.createtimes_shelf.get(str((name, node)), None)
    def get_filename(self, name, node):
        if (name, node) in self.aliases:
            return self.get_filename(*self.aliases[(name, node)])
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
    def register_disposable(self, name, node, filename):
        self.disposable[(name, node)].add(filename)
    def cleanup(self, depgraph):
        for name, node in set(depgraph.obsolete):
            if (name, node) in self.aliases:
                continue
            alias_ids = set([(name, node)] + list(self.get_aliases(name, node)))
            if alias_ids.issubset(depgraph.obsolete):
                for filename in self.disposable.get((name, node), ()):
                    if os.path.exists(filename):
                        os.remove(filename)
            depgraph.obsolete.remove((name, node))

