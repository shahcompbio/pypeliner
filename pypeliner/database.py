import collections
import errno
import itertools
import logging
import shutil

from pypeliner.sqlitedb import SqliteDb
import os

import pypeliner.helpers
import pypeliner.identifiers
import pypeliner.resources
import pypeliner.workflow


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
                for node in self.retrieve_nodes(
                        axes[1:], base_node + pypeliner.identifiers.AxisInstance(axes[0], chunk)
                ):
                    yield node

    def get_chunks_filename(self, axis, node):
        return os.path.join(self.nodes_dir, node.subdir, axis + '_chunks')

    def retrieve_chunks(self, axes, node):
        assert isinstance(axes, tuple)
        if len(axes) == 0:
            yield ()
        else:
            for chunk in self.retrieve_axis_chunks(axes[0], node):
                for chunks_rest in self.retrieve_chunks(
                        axes[1:], node + pypeliner.identifiers.AxisInstance(axes[0], chunk)
                ):
                    yield (chunk,) + chunks_rest

    def retrieve_axis_chunks(self, axis, node):
        if (axis, node) not in self.cached_chunks:
            filename = self.db.get_temp_filename(axis, node)
            resource = pypeliner.resources.TempObjManager(self.db.file_storage, axis, node, filename)
            chunks = resource.get_obj()
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
        for level in range(len(axes)):
            if level not in subset:
                continue
            for pre_chunks, level_chunks in itertools.groupby(sorted(chunks), lambda a: a[:level]):
                level_node = node
                for idx in range(level):
                    level_node += pypeliner.identifiers.AxisInstance(axes[idx], pre_chunks[idx])
                level_chunks = set([a[level] for a in level_chunks])
                self.store_axis_chunks(axes[level], level_node, level_chunks)

    def store_axis_chunks(self, axis, node, chunks):
        for chunk in chunks:
            new_node = node + pypeliner.identifiers.AxisInstance(axis, chunk)
        chunks = sorted(chunks)
        self.cached_chunks[(axis, node)] = chunks
        filename = self.db.get_temp_filename(axis, node)
        resource = pypeliner.resources.TempObjManager(self.db.file_storage, axis, node, filename)
        resource.finalize(chunks)

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
            filename = self.db.get_temp_filename(axes[0], node)
            yield pypeliner.resources.TempObjManager(self.db.file_storage, axes[0], node, filename)
        for level in range(1, len(axes)):
            if level not in subset:
                continue
            for level_node in self.retrieve_nodes(axes[:level], base_node=node):
                filename = self.db.get_temp_filename(axes[level], level_node)
                yield pypeliner.resources.TempObjManager(self.db.file_storage, axes[level], level_node, filename)

    def get_node_inputs(self, node):
        if len(node) >= 1:
            yield pypeliner.resources.Dependency(node[-1][0], node[:-1])


def _check_template(template, node):
    for axis, _ in node:
        if axis not in template:
            raise ValueError('filename template {} does not contain {{{}}}'.format(template, axis))


def resolve_user_filename(name, node, path_info):
    """ Resolve a filename based on user provided information """
    if path_info.fnames is not None:
        fname_key = tuple([a[1] for a in node])
        if len(fname_key) == 1:
            fname_key = fname_key[0]
        filename = path_info.fnames.get(fname_key)
    elif path_info.template is not None:
        _check_template(path_info.template, node)
        filename = path_info.template.format(**dict(node))
    else:
        _check_template(name, node)
        try:
            filename = name.format(**dict(node))
        except KeyError as e:
            raise ValueError('template {} contains {}'.format(name, e.args[0]))
    return filename


class UserFilenameCreator(object):
    """ Function object for creating user filenames from name node pairs """

    def __init__(self, path_info):
        self.path_info = path_info

    def __call__(self, name, node):
        return resolve_user_filename(name, node, self.path_info)

    def __repr__(self):
        return '{0}.{1}({2})'.format(
            UserFilenameCreator.__module__,
            UserFilenameCreator.__name__,
            repr(self.path_info))


class TempFilenameCreator(object):
    """ Function object for creating filenames from name node pairs """

    def __init__(self, file_dir=''):
        self.file_dir = file_dir

    def __call__(self, name, node):
        return os.path.join(self.file_dir, node.subdir, name)

    def __repr__(self):
        return '{0}.{1}({2})'.format(
            TempFilenameCreator.__module__,
            TempFilenameCreator.__name__,
            self.file_dir)


class WorkflowDatabase(object):
    def __init__(self, temps_dir, workflow_dir, logs_dir, file_storage, job_shelf, path_info, instance_subdir):
        self.file_storage = file_storage
        self.job_shelf = job_shelf
        self.path_info = path_info
        self.instance_subdir = instance_subdir
        self.envs_dir = os.path.join(workflow_dir, 'envs')
        self.nodes_dir = os.path.join(workflow_dir, 'nodes', instance_subdir)
        self.temps_dir = os.path.join(temps_dir, instance_subdir)
        self.nodemgr = NodeManager(self, self.nodes_dir, self.temps_dir)
        self.logs_dir = os.path.join(logs_dir, instance_subdir)

    def get_user_filename_creator(self, name, axes, fnames=None, template=None):
        arg_path_info = pypeliner.workflow.UserPathInfo(fnames=fnames, template=template)
        if (name, axes) in self.path_info:
            user_path_info = self.path_info[(name, axes)]
            if arg_path_info.fnames is not None and arg_path_info.template is not None and arg_path_info != user_path_info:
                raise Exception('{} doesnt equal {}'.format(arg_path_info, user_path_info))
        else:
            user_path_info = arg_path_info
        return UserFilenameCreator(user_path_info)

    def get_user_filename(self, name, node, fnames=None, template=None):
        return self.get_user_filename_creator(name, node.axes, fnames=fnames, template=template)(name, node)

    def get_temp_filename_creator(self):
        return TempFilenameCreator(file_dir=self.temps_dir)

    def get_temp_filename(self, name, node):
        if os.path.isabs(name):
            raise Exception('name {} is an absolute path'.format(name))
        return self.get_temp_filename_creator()(name, node)


class PipelineLockedError(Exception):
    pass


class WorkflowDatabaseFactory(object):
    def __init__(self, temps_dir, workflow_dir, logs_dir, file_storage):
        self.temps_dir = temps_dir
        self.workflow_dir = workflow_dir
        self.logs_dir = logs_dir
        pypeliner.helpers.makedirs(self.workflow_dir)
        self.file_storage = file_storage
        self.job_shelf_filename = os.path.join(self.workflow_dir, 'jobs.db')
        self.lock_directories = list()

    def create(self, path_info, instance_subdir):
        self._add_lock(instance_subdir)
        db = WorkflowDatabase(
            self.temps_dir, self.workflow_dir, self.logs_dir, self.file_storage,
            self.job_shelf, path_info, instance_subdir)
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
        self.job_shelf = SqliteDb(self.job_shelf_filename)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.job_shelf.close()
        for lock_directory in self.lock_directories:
            try:
                os.rmdir(lock_directory)
            except:
                logging.exception('unable to unlock ' + lock_directory)
