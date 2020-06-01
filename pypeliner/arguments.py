import os

import pypeliner.helpers
import pypeliner.identifiers
import pypeliner.resources


class Arg(object):
    is_split = False

    def get_inputs(self):
        return []

    def get_merge_inputs(self):
        return []

    def get_outputs(self):
        return []

    def get_split_outputs(self):
        return []

    def resolve(self):
        return None

    def update(self, job):
        pass

    def finalize(self, v):
        pass

    def allocate(self):
        pass

    def pull(self):
        pass

    def push(self):
        pass


class SplitMergeArg(object):
    def get_node_chunks(self, node):
        chunks = tuple([a[1] for a in node[-len(self.axes):]])
        if len(chunks) == 1:
            return chunks[0]
        return chunks

    def get_axes_origin(self, axes_origin):
        if axes_origin is None:
            return set(range(len(self.axes)))
        return axes_origin


class TemplateArg(Arg):
    """ Templated name argument

    The name parameter is treated as a string with named formatting.  Resolves to the name formatted using the node
    dictionary.

    """

    def __init__(self, db, name, node, template=None, **kwargs):
        self.name = name
        self.node = node
        if template is not None:
            self.filename = template.format(**dict(node))
        else:
            self.filename = name.format(**dict(node))

    def resolve(self):
        return self.filename


class TempSpaceArg(Arg):
    """ Temporary space argument

    Resolves to a filename/directory contained within the temporary files directory.

    """

    def __init__(self, db, name, node, cleanup='both', **kwargs):
        self.name = name
        self.node = node
        self.cleanup = cleanup
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.UserResource(
            db.file_storage, name, node, filename,
            direct_write=kwargs.get('direct_write'),
            store_dir=kwargs.get('store_dir'),
            extensions=kwargs.get('extensions'))
        self.filename = self.resource.filename

    def allocate(self):
        self.resource.allocate()

    def get_outputs(self):
        yield pypeliner.resources.Dependency(self.name, self.node)

    def resolve(self):
        if self.node.undefined:
            raise Exception('Undefined node {} for {}'.format(self.node.displayname, self.name))
        return self.filename

    def pull(self):
        if self.cleanup in ('before', 'both'):
            pypeliner.helpers.removefiledir(self.filename)
        pypeliner.helpers.makedirs(os.path.dirname(self.filename))

    def push(self):
        if self.cleanup in ('after', 'both'):
            pypeliner.helpers.removefiledir(self.filename)


class MergeTemplateArg(Arg, SplitMergeArg):
    """ Temp input files merged along a single axis

    The name parameter is treated as a string with named formatting.  Resolves to a dictionary with the keys as chunks
    for the merge axis.  Each value is the name formatted using the merge node dictionary.

    """

    def __init__(self, db, name, node, axes, template=None, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.template = template
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node))
        self.formatted = {}
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            if self.template is not None:
                self.formatted[self.get_node_chunks(node)] = self.template.format(**dict(node))
            else:
                self.formatted[self.get_node_chunks(node)] = self.name.format(**dict(node))

    def get_merge_inputs(self):
        return self.merge_inputs

    def resolve(self):
        return self.formatted


class InputFileArg(Arg):
    """ Input file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary.

    """

    def __init__(self, db, name, node, fnames=None, template=None, **kwargs):
        filename = db.get_user_filename(name, node, fnames=fnames, template=template)
        self.resource = pypeliner.resources.UserResource(
            db.file_storage, name, node, filename,
            direct_write=kwargs.get('direct_write'),
            store_dir=kwargs.get('store_dir'),
            extensions=kwargs.get('extensions'))

    def get_inputs(self):
        yield self.resource

    def resolve(self):
        return self.resource.filename

    def allocate(self):
        self.resource.allocate()

    def pull(self):
        self.resource.pull()


class MergeFileArg(Arg, SplitMergeArg):
    """ Input files merged along a single axis

    The name argument is treated as a filename with named formatting.  Resolves to a dictionary with keys as chunks for
    the merge axis.  Each value is the filename formatted using the merge node dictonary.

    """

    def __init__(self, db, name, node, axes, fnames=None, template=None, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.fnames = fnames
        self.template = template
        self.resources = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_user_filename(self.name, node, fnames=self.fnames, template=self.template)
            resource = pypeliner.resources.UserResource(
                db.file_storage, self.name, node, filename,
                direct_write=kwargs.get('direct_write'),
                store_dir=kwargs.get('store_dir'),
                extensions=kwargs.get('extensions'))
            self.resources.append(resource)
        self.merge_inputs = []
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            self.merge_inputs.append(dependency)

    def get_inputs(self):
        return self.resources

    def get_merge_inputs(self):
        return self.merge_inputs

    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            resolved[self.get_node_chunks(resource.node)] = resource.filename
        return resolved

    def allocate(self):
        for resource in self.resources:
            resource.allocate()

    def pull(self):
        for resource in self.resources:
            resource.pull()


class OutputFileArg(Arg):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """

    def __init__(self, db, name, node, fnames=None, template=None, **kwargs):
        filename = db.get_user_filename(name, node, fnames=fnames, template=template)
        self.resource = pypeliner.resources.UserResource(
            db.file_storage, name, node, filename,
            direct_write=kwargs.get('direct_write'),
            store_dir=kwargs.get('store_dir'),
            extensions=kwargs.get('extensions'))

    def get_outputs(self):
        yield self.resource

    def resolve(self):
        return self.resource.write_filename

    def allocate(self):
        self.resource.allocate()

    def push(self):
        self.resource.push()


class SplitFileArg(Arg, SplitMergeArg):
    """ Output file arguments from a split

    The name argument is treated as a filename with named formatting.  Resolves to a filename callback that can be used
    to generate filenames based on a given split axis chunk.  Resolved filenames have the '.tmp' suffix.  Finalizing
    involves removing the '.tmp' suffix for each file created by the job.

    """

    def __init__(self, db, name, node, axes, axes_origin=None, fnames=None, template=None, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.axes_origin = self.get_axes_origin(axes_origin)
        self.is_split = len(self.axes_origin) != 0
        self.fnames = fnames
        self.template = template
        self.resources = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_user_filename(self.name, node, fnames=self.fnames, template=self.template)
            resource = pypeliner.resources.UserResource(
                db.file_storage, self.name, node, filename,
                direct_write=kwargs.get('direct_write'),
                store_dir=kwargs.get('store_dir'),
                extensions=kwargs.get('extensions'))
            self.resources.append(resource)
        self.split_outputs = list(db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin))
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin))
        filename_creator = db.get_user_filename_creator(
            self.name, self.node.axes + self.axes, fnames=self.fnames, template=self.template)
        self.filename_callback = UserFilenameCallback(
            db.file_storage, self.name, self.node, self.axes, filename_creator, **kwargs)

    def get_merge_inputs(self):
        return self.merge_inputs

    def get_outputs(self):
        return self.resources

    def get_split_outputs(self):
        return self.split_outputs

    def resolve(self):
        return self.filename_callback

    def update(self, job):
        if self.is_split:
            job.workflow.db.nodemgr.store_chunks(self.axes, self.node, list(self.filename_callback.resources.keys()), subset=self.axes_origin)

    def push(self):
        for resource in self.filename_callback.resources.values():
            resource.push()


class TempInputObjArg(Arg):
    """ Temporary input object argument

    Resolves to an object.  If func is given, resolves to the return value of func called with object as the only
    parameter.

    """

    def __init__(self, db, name, node, func=None, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempObjManager(db.file_storage, name, node, filename)
        self.func = func

    def get_inputs(self):
        yield self.resource.input

    def resolve(self):
        obj = self.resource.get_obj()
        if self.func is not None:
            obj = self.func(obj)
        return obj


class TempMergeObjArg(Arg, SplitMergeArg):
    """ Temp input object arguments merged along single axis

    Resolves to an dictionary of objects with keys given by the merge axis chunks.

    """

    def __init__(self, db, name, node, axes, func=None, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.func = func
        self.resources = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempObjManager(db.file_storage, self.name, node, filename)
            self.resources.append(resource)
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node))

    def get_inputs(self):
        for resource in self.resources:
            yield resource.input

    def get_merge_inputs(self):
        return self.merge_inputs

    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            obj = resource.get_obj()
            if self.func is not None:
                obj = self.func(obj)
            resolved[self.get_node_chunks(resource.node)] = obj
        return resolved


class TempOutputObjArg(Arg):
    """ Temporary output object argument

    Stores an object created by a job.

    """

    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempObjManager(db.file_storage, name, node, filename)

    def get_outputs(self):
        yield self.resource.output

    def resolve(self):
        return self

    def finalize(self, value):
        self.resource.finalize(value)


class TempSplitObjArg(Arg, SplitMergeArg):
    """ Temporary output object arguments from a split

    Stores a dictionary of objects created by a job.  The keys of the dictionary are taken as the chunks for the given
    split axis.

    """

    def __init__(self, db, name, node, axes, axes_origin=None, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.axes_origin = self.get_axes_origin(axes_origin)
        self.is_split = len(self.axes_origin) != 0
        self.resources = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempObjManager(db.file_storage, self.name, node, filename)
            self.resources.append(resource)
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin))
        self.split_outputs = list(db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin))
        self.storage = db.file_storage
        self.filename_creator = db.get_temp_filename_creator()

    def get_merge_inputs(self):
        return self.merge_inputs

    def get_outputs(self):
        for resource in self.resources:
            yield resource.output

    def get_split_outputs(self):
        return self.split_outputs

    def resolve(self):
        return self

    def finalize(self, values):
        self.chunks_list = list(values.keys())
        self.resources = []
        for chunks, value in values.items():
            if not isinstance(chunks, tuple):
                chunks = (chunks,)
            if len(self.axes) != len(chunks):
                raise ValueError('expected ' + str(len(self.axes)) + ' values for axes ' + str(self.axes))
            node = self.node
            for axis, chunk in zip(self.axes, chunks):
                node += pypeliner.identifiers.AxisInstance(axis, chunk)
            filename = self.filename_creator(self.name, node)
            resource = pypeliner.resources.TempObjManager(self.storage, self.name, node, filename)
            resource.finalize(value)
            self.resources.append(resource)

    def update(self, job):
        job.workflow.db.nodemgr.store_chunks(self.axes, self.node, self.chunks_list, subset=self.axes_origin)


class TempInputFileArg(Arg):
    """ Temp input file argument

    Resolves to a filename for a temporary file.

    """

    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempFileResource(
            db.file_storage, name, node, filename,
            direct_write=kwargs.get('direct_write'),
            store_dir=kwargs.get('store_dir'),
            extensions=kwargs.get('extensions'))

    def get_inputs(self):
        yield self.resource

    def resolve(self):
        return self.resource.filename

    def allocate(self):
        self.resource.allocate()

    def pull(self):
        self.resource.pull()


class TempMergeFileArg(Arg, SplitMergeArg):
    """ Temp input files merged along a single axis

    Resolves to a dictionary of filenames of temporary files.

    """

    def __init__(self, db, name, node, axes, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.resources = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempFileResource(
                db.file_storage, self.name, node, filename,
                direct_write=kwargs.get('direct_write'),
                store_dir=kwargs.get('store_dir'),
                extensions=kwargs.get('extensions'))
            self.resources.append(resource)
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node))

    def get_inputs(self):
        return self.resources

    def get_merge_inputs(self):
        return self.merge_inputs

    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            resolved[self.get_node_chunks(resource.node)] = resource.filename
        return resolved

    def allocate(self):
        for resource in self.resources:
            resource.allocate()

    def pull(self):
        for resource in self.resources:
            resource.pull()


class TempOutputFileArg(Arg):
    """ Temp output file argument

    Resolves to an output filename for a temporary file.  Finalizes with resource manager.

    """

    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempFileResource(
            db.file_storage, name, node, filename,
            direct_write=kwargs.get('direct_write'),
            store_dir=kwargs.get('store_dir'),
            extensions=kwargs.get('extensions'))

    def get_outputs(self):
        yield self.resource

    def resolve(self):
        return self.resource.write_filename

    def allocate(self):
        self.resource.allocate()

    def push(self):
        self.resource.push()


class FilenameCallback(object):
    """ Argument to split jobs providing callback for filenames
    with a particular instance """

    def __init__(self, storage, name, node, axes, filename_creator, **kwargs):
        self.storage = storage
        self.name = name
        self.node = node
        self.axes = axes
        self.filename_creator = filename_creator
        self.resources = dict()
        self.kwargs = kwargs

    def get_filename(self, *chunks):
        if len(self.axes) != len(chunks):
            raise ValueError('expected ' + str(len(self.axes)) + ' values for axes ' + str(self.axes))
        node = self.node
        for axis, chunk in zip(self.axes, chunks):
            node += pypeliner.identifiers.AxisInstance(axis, chunk)
        filename = self.filename_creator(self.name, node)
        resource = self.create_resource(
            self.storage, self.name, node, filename,
            direct_write=self.kwargs.get('direct_write'),
            store_dir=self.kwargs.get('store_dir'),
            extensions=self.kwargs.get('extensions'))
        return resource.write_filename

    def __call__(self, *chunks):
        return self.__getitem__(*chunks)

    def __getitem__(self, *chunks):
        if len(self.axes) != len(chunks):
            raise ValueError('expected ' + str(len(self.axes)) + ' values for axes ' + str(self.axes))
        node = self.node
        for axis, chunk in zip(self.axes, chunks):
            node += pypeliner.identifiers.AxisInstance(axis, chunk)
        filename = self.filename_creator(self.name, node)
        resource = self.create_resource(
            self.storage, self.name, node, filename,
            direct_write=self.kwargs.get('direct_write'),
            store_dir=self.kwargs.get('store_dir'),
            extensions=self.kwargs.get('extensions'))
        if len(chunks) == 1:
            self.resources[chunks[0]] = resource
        else:
            self.resources[chunks] = resource
        resource.allocate()
        return resource.write_filename

    def __repr__(self):
        return '{0}.{1}({2},{3},{4},{5})'.format(
            FilenameCallback.__module__,
            FilenameCallback.__name__,
            self.name,
            self.node,
            self.axes,
            self.filename_creator)


class TempFilenameCallback(FilenameCallback):
    create_resource = pypeliner.resources.TempFileResource


class UserFilenameCallback(FilenameCallback):
    create_resource = pypeliner.resources.UserResource


class TempSplitFileArg(Arg, SplitMergeArg):
    """ Temp output file arguments from a split

    Resolves to a filename callback that can be used to create a temporary filename for each chunk of the split on the
    given axis.  Finalizes with resource manager to move from temporary filename to final filename.

    """

    def __init__(self, db, name, node, axes, axes_origin=None, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.axes_origin = self.get_axes_origin(axes_origin)
        self.is_split = len(self.axes_origin) != 0
        self.resources = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempFileResource(
                db.file_storage, self.name, node, filename,
                direct_write=kwargs.get('direct_write'),
                store_dir=kwargs.get('store_dir'),
                extensions=kwargs.get('extensions'))
            self.resources.append(resource)
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin))
        self.split_outputs = list(db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin))
        self.filename_callback = TempFilenameCallback(
            db.file_storage, self.name, self.node, self.axes,
            db.get_temp_filename_creator(), **kwargs)

    def get_merge_inputs(self):
        return self.merge_inputs

    def get_outputs(self):
        return self.resources

    def get_split_outputs(self):
        return self.split_outputs

    def resolve(self):
        return self.filename_callback

    def update(self, job):
        if self.is_split:
            job.workflow.db.nodemgr.store_chunks(self.axes, self.node, list(self.filename_callback.resources.keys()), subset=self.axes_origin)

    def push(self):
        for resource in self.filename_callback.resources.values():
            resource.push()


class InputInstanceArg(Arg):
    """ Instance of a job as an argument

    Resolves to the instance of the given job for a specific axis.

    """

    def __init__(self, db, node, axis, **kwargs):
        self.chunk = dict(node)[axis]

    def resolve(self):
        return self.chunk


class InputChunksArg(Arg):
    """ Instance list of an axis as an argument

    Resolves to the list of chunks for the given axes.

    """

    def __init__(self, db, name, node, axis, **kwargs):
        self.node = node
        self.axis = axis
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axis, self.node))
        self.chunks = list(db.nodemgr.retrieve_chunks(self.axis, self.node))
        if len(self.axis) == 1:
            self.chunks = [chunk[0] for chunk in self.chunks]

    def get_merge_inputs(self):
        return self.merge_inputs

    def resolve(self):
        return self.chunks


class OutputChunksArg(Arg, SplitMergeArg):
    """ Instance list of a job as an argument

    Sets the list of chunks for the given axes.

    """

    def __init__(self, db, name, node, axes, axes_origin=None, **kwargs):
        self.node = node
        self.axes = axes
        self.axes_origin = self.get_axes_origin(axes_origin)
        self.is_split = len(self.axes_origin) != 0
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin))
        self.split_outputs = list(db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin))

    def get_merge_inputs(self):
        return self.merge_inputs

    def get_split_outputs(self):
        return self.split_outputs

    def resolve(self):
        return self

    def finalize(self, value):
        self.chunks_list = value

    def update(self, job):
        job.workflow.db.nodemgr.store_chunks(self.axes, self.node, self.chunks_list, subset=self.axes_origin)


class OutputWorkflowArg(Arg):
    """ Output workflow object argument

    Stores a workflow object created by a job.

    """

    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempObjManager(db.file_storage, name, node, filename)

    def get_outputs(self):
        yield self.resource.output

    def resolve(self):
        return self

    def finalize(self, value):
        self.resource.finalize(value)

    def get_obj(self):
        return self.resource.get_obj()
