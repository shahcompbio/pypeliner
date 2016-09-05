import collections
import errno
import itertools
import os
import pickle
import logging
import shutil

import pypeliner.helpers
import pypeliner.resources
import pypeliner.identifiers

class NodeManager(object):
    """ Manages nodes in the underlying pipeline graph """
    def __init__(self, db, nodes_dir, temps_dir):
        self.db = db
        self.nodes_dir = nodes_dir
        self.temps_dir = temps_dir
        self.cached_chunks = dict()
    def retrieve_nodes(self, axes, base_node=None):
        if base_node is None:
            base_node = pypeliner.identifiers.Node()
        assert isinstance(base_node, pypeliner.identifiers.Node)
        if len(axes) == 0:
            yield base_node
        else:
            for chunk in self.retrieve_axis_chunks(axes[0], base_node):
                for node in self.retrieve_nodes(axes[1:], base_node + pypeliner.identifiers.AxisInstance(axes[0], chunk)):
                    yield node
    def get_chunks_filename(self, axis, node):
        return os.path.join(self.nodes_dir, node.subdir, axis+'_chunks')
    def retrieve_chunks(self, axes, node):
        assert isinstance(axes, tuple)
        if len(axes) == 0:
            yield ()
        else:
            for chunk in self.retrieve_axis_chunks(axes[0], node):
                for chunks_rest in self.retrieve_chunks(axes[1:], node + pypeliner.identifiers.AxisInstance(axes[0], chunk)):
                    yield (chunk,) + chunks_rest
    def retrieve_axis_chunks(self, axis, node):
        if (axis, node) not in self.cached_chunks:
            resource = pypeliner.resources.TempObjManager(self.db, axis, node, temps_dir=self.db.temps_dir)
            chunks = resource.get_obj(self.db)
            if chunks is None:
                chunks = (None,)
            self.cached_chunks[(axis, node)] = chunks
        return self.cached_chunks[(axis, node)]
    def store_chunks(self, axes, node, chunks, subset=None):
        if subset is None:
            subset = set([])
        if len(chunks) == 0:
            raise ValueError('must be at least one chunk per axis')
        def _convert_tuple(a):
            if isinstance(a, tuple):
                return a
            else:
                return tuple([a])
        chunks = [_convert_tuple(a) for a in chunks]
        if len(axes) != len(chunks[0]):
            raise ValueError('for multiple axis, chunks must be a tuple of the same length')
        for level in xrange(len(axes)):
            if level not in subset:
                continue
            for pre_chunks, level_chunks in itertools.groupby(sorted(chunks), lambda a: a[:level]):
                level_node = node
                for idx in xrange(level):
                    level_node += pypeliner.identifiers.AxisInstance(axes[idx], pre_chunks[idx])
                level_chunks = set([a[level] for a in level_chunks])
                self.store_axis_chunks(axes[level], level_node, level_chunks)
    def store_axis_chunks(self, axis, node, chunks):
        for chunk in chunks:
            new_node = node + pypeliner.identifiers.AxisInstance(axis, chunk)
            pypeliner.helpers.makedirs(os.path.join(self.db.resmgr.temps_dir, new_node.subdir))
        chunks = sorted(chunks)
        self.cached_chunks[(axis, node)] = chunks
        resource = pypeliner.resources.TempObjManager(self.db, axis, node, temps_dir=self.db.temps_dir)
        resource.finalize(chunks, self.db)
    def get_merge_inputs(self, axes, node, subset=None):
        if subset is None:
            subset = set([])
        subset = set(range(len(axes))).difference(subset)
        resources = self.get_chunks_resource(axes, node, subset)
        inputs = [resource.input for resource in resources]
        return inputs
    def get_split_outputs(self, axes, node, subset=None):
        if subset is None:
            subset = set([])
        resources = self.get_chunks_resource(axes, node, subset)
        outputs = [resource.output for resource in resources]
        return outputs
    def get_chunks_resource(self, axes, node, subset):
        if 0 in subset:
            yield pypeliner.resources.TempObjManager(self.db, axes[0], node, temps_dir=self.db.temps_dir)
        for level in xrange(1, len(axes)):
            if level not in subset:
                continue
            for level_node in self.retrieve_nodes(axes[:level], base_node=node):
                yield pypeliner.resources.TempObjManager(self.db, axes[level], level_node, temps_dir=self.db.temps_dir)
    def get_node_inputs(self, node):
        if len(node) >= 1:
            yield pypeliner.resources.Dependency(self.db, node[-1][0], node[:-1])

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
    def __init__(self, temps_dir):
        self.temps_dir = temps_dir
        self.disposable = collections.defaultdict(set)
    def register_disposable(self, name, node, filename):
        self.disposable[(name, node)].add(filename)
    def cleanup(self, depgraph):
        for name, node in set(depgraph.obsolete):
            for filename in self.disposable.get((name, node), ()):
                if os.path.exists(filename):
                    logging.getLogger('resourcemgr').debug('removing ' + filename)
                    os.remove(filename)
            depgraph.obsolete.remove((name, node))

class WorkflowDatabase(object):
    def __init__(self, workflow_dir, logs_dir, instance_subdir):
        self.instance_subdir = instance_subdir
        self.nodes_dir = os.path.join(workflow_dir, 'nodes', instance_subdir)
        self.temps_dir = os.path.join(workflow_dir, 'tmp', instance_subdir)
        pypeliner.helpers.makedirs(self.nodes_dir)
        pypeliner.helpers.makedirs(self.temps_dir)
        self.resmgr = ResourceManager(self.temps_dir)
        self.nodemgr = NodeManager(self, self.nodes_dir, self.temps_dir)
        self.logs_dir = os.path.join(logs_dir, instance_subdir)
        self.resources = {}
        self.resource_types = {}
    def create_resource(self, typ, name, node, **kwargs):
        if (name, node) not in self.resources:
            self.resources[(name, node)] = typ(self, name, node, temps_dir=self.temps_dir, **kwargs)
            self.resource_types[(name, node)] = typ
        if typ != self.resource_types[(name, node)]:
            raise Exception('resource {}, {} with type {} and {} in workflow {}'.format(
                name, node, typ.__name__, resource_types[(name, node)], instance_subdir))
        return self.resources[(name, node)]

class PipelineLockedError(Exception):
    pass

class WorkflowDatabaseFactory(object):
    def __init__(self, workflow_dir, logs_dir):
        self.workflow_dir = workflow_dir
        self.logs_dir = logs_dir
        self.lock_directories = list()
    def create(self, instance_subdir):
        self._add_lock(instance_subdir)
        db = WorkflowDatabase(self.workflow_dir, self.logs_dir, instance_subdir)
        return db
    def _add_lock(self, instance_subdir):
        lock_directory = os.path.join(self.workflow_dir, 'locks', instance_subdir, '_lock')
        try:
            pypeliner.helpers.makedirs(os.path.join(lock_directory, os.path.pardir))
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
        for lock_directory in self.lock_directories:
            try:
                os.rmdir(lock_directory)
            except:
                logging.exception('unable to unlock ' + lock_directory)



