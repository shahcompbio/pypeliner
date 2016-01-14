import collections
import errno
import itertools
import os
import errno
import pickle
import shelve
import logging

import helpers
import resources
import managed
import identifiers

class NodeManager(object):
    """ Manages nodes in the underlying pipeline graph """
    def __init__(self, nodes_dir, temps_dir):
        self.nodes_dir = nodes_dir
        self.temps_dir = temps_dir
        self.cached_chunks = dict()
    def retrieve_nodes(self, axes, base_node=None):
        if base_node is None:
            base_node = identifiers.Node()
        assert isinstance(base_node, identifiers.Node)
        if len(axes) == 0:
            yield base_node
        else:
            for chunk in self.retrieve_axis_chunks(axes[0], base_node):
                for node in self.retrieve_nodes(axes[1:], base_node + identifiers.AxisInstance(axes[0], chunk)):
                    yield node
    def get_chunks_filename(self, axis, node):
        return os.path.join(self.nodes_dir, node.subdir, axis+'_chunks')
    def retrieve_chunks(self, axes, node):
        assert isinstance(axes, tuple)
        if len(axes) == 0:
            yield ()
        else:
            for chunk in self.retrieve_axis_chunks(axes[0], node):
                for chunks_rest in self.retrieve_chunks(axes[1:], node + identifiers.AxisInstance(axes[0], chunk)):
                    yield (chunk,) + chunks_rest
    def retrieve_axis_chunks(self, axis, node):
        if (axis, node) not in self.cached_chunks:
            chunks_filename = self.get_chunks_filename(axis, node)
            if not os.path.exists(chunks_filename):
                return (None,)
            else:
                with open(chunks_filename, 'rb') as f:
                    self.cached_chunks[(axis, node)] = pickle.load(f)
        return self.cached_chunks[(axis, node)]
    def store_chunks(self, axes, node, chunks, subset=None):
        if subset is None:
            subset = set([])
        if len(chunks) == 0:
            raise ValueError('must be at least one chunk per axis')
        if not isinstance(chunks[0], tuple):
            chunks = [tuple([a]) for a in chunks]
        if len(axes) != len(chunks[0]):
            raise ValueError('for multiple axis, chunks must be a tuple of the same length')
        for level in xrange(len(axes)):
            if level not in subset:
                continue
            for pre_chunks, level_chunks in itertools.groupby(chunks, lambda a: a[:level]):
                level_node = node
                for idx in xrange(level):
                    level_node += identifiers.AxisInstance(axes[idx], pre_chunks[idx])
                level_chunks = set([a[level] for a in level_chunks])
                self.store_axis_chunks(axes[level], level_node, level_chunks)
    def store_axis_chunks(self, axis, node, chunks):
        for chunk in chunks:
            new_node = node + identifiers.AxisInstance(axis, chunk)
            helpers.makedirs(os.path.join(self.temps_dir, new_node.subdir))
        chunks = sorted(chunks)
        self.cached_chunks[(axis, node)] = chunks
        chunks_filename = self.get_chunks_filename(axis, node)
        helpers.makedirs(os.path.dirname(chunks_filename))
        temp_chunks_filename = chunks_filename + '.tmp'
        with open(temp_chunks_filename, 'wb') as f:
            pickle.dump(chunks, f)
        helpers.overwrite_if_different(temp_chunks_filename, chunks_filename)
    def get_merge_inputs(self, axes, node, subset=None):
        if subset is None:
            subset = set([])
        subset = set(range(len(axes))).difference(subset)
        return self.get_splitmerge(axes, node, subset, resources.ChunksResource)
    def get_split_outputs(self, axes, node, subset=None):
        if subset is None:
            subset = set([])
        return self.get_splitmerge(axes, node, subset, resources.Dependency)
    def get_splitmerge(self, axes, node, subset, factory):
        if 0 in subset:
            yield factory(axes[0], node)
        for level in xrange(1, len(axes)):
            if level not in subset:
                continue
            for level_node in self.retrieve_nodes(axes[:level], base_node=node):
                yield factory(axes[level], level_node)
    def get_node_inputs(self, node):
        if len(node) >= 1:
            yield resources.Dependency(node[-1][0], node[:-1])
    def close(self):
        pass

class FilenameCreator(object):
    """ Function object for creating filenames from name node pairs """
    def __init__(self, file_dir='', file_suffix=''):
        self.file_dir = file_dir
        self.file_suffix = file_suffix
    def __call__(self, name, node):
        return os.path.join(self.file_dir, node.subdir, name + self.file_suffix)
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCreator.__module__, FilenameCreator.__name__, ', '.join(repr(a) for a in (self.file_dir, self.file_suffix)))

class ResourceManager(object):
    """ Manages file resources """
    def __init__(self, temps_dir, db_dir):
        self.temps_dir = temps_dir
        self.db_dir = db_dir
        self.disposable = collections.defaultdict(set)
        self.aliases = dict()
        self.rev_alias = collections.defaultdict(list)
        self.createtimes_shelf = shelve.open(os.path.join(self.db_dir, 'createtimes'))
    def get_filename_creator(self, suffix):
        return FilenameCreator(self.temps_dir, suffix)
    def store_createtime(self, name, node, filename):
        self.createtimes_shelf[str((name, node))] = os.path.getmtime(filename)
    def retrieve_createtime(self, name, node, filename):
        if os.path.exists(filename):
            self.createtimes_shelf[str((name, node))] = os.path.getmtime(filename)
        return self.createtimes_shelf.get(str((name, node)), None)
    def get_filename(self, name, node):
        if (name, node) in self.aliases:
            return self.get_filename(*self.aliases[(name, node)])
        else:
            return os.path.join(self.temps_dir, node.subdir, name)
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
                        logging.getLogger('resourcemgr').debug('removing ' + filename)
                        os.remove(filename)
            depgraph.obsolete.remove((name, node))
    def close(self):
        self.createtimes_shelf.close()

class WorkflowDatabase(object):
    def __init__(self, workflow_dir, logs_dir, instance_subdir):
        self.instance_subdir = instance_subdir
        db_dir = os.path.join(workflow_dir, 'db', instance_subdir)
        nodes_dir = os.path.join(workflow_dir, 'nodes', instance_subdir)
        temps_dir = os.path.join(workflow_dir, 'tmp', instance_subdir)
        helpers.makedirs(db_dir)
        helpers.makedirs(nodes_dir)
        helpers.makedirs(temps_dir)
        self.resmgr = ResourceManager(temps_dir, db_dir)
        self.nodemgr = NodeManager(nodes_dir, temps_dir)
        self.logs_dir = os.path.join(logs_dir, instance_subdir)
    def close(self):
        self.resmgr.close()
        self.nodemgr.close()

class PipelineLockedError(Exception):
    pass

class WorkflowDatabaseFactory(object):
    def __init__(self, workflow_dir, logs_dir):
        self.workflow_dir = workflow_dir
        self.logs_dir = logs_dir
        self.dbs = list()
        self.lock_directories = list()
    def create(self, instance_subdir):
        self._add_lock(instance_subdir)
        db = WorkflowDatabase(self.workflow_dir, self.logs_dir, instance_subdir)
        self.dbs.append(db)
        return db
    def _add_lock(self, instance_subdir):
        lock_directory = os.path.join(self.workflow_dir, 'locks', instance_subdir, '_lock')
        try:
            helpers.makedirs(os.path.join(lock_directory, os.path.pardir))
            os.mkdir(lock_directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise PipelineLockedError('Pipeline already running, remove {0} to override'.format(lock_directory))
            else:
                raise
        self.lock_directories.append(lock_directory)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        for db in self.dbs:
            try:
                db.close()
            except:
                logging.exception('unable to close ' + db.instance_subdir)
        for lock_directory in self.lock_directories:
            try:
                os.rmdir(lock_directory)
            except:
                logging.exception('unable to unlock ' + lock_directory)



