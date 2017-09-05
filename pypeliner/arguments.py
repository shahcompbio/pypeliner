import os

import pypeliner.helpers
import pypeliner.resources
import pypeliner.identifiers


class Arg(object):
    is_split = False
    def get_inputs(self):
        return []
    def get_outputs(self):
        return []
    def resolve(self):
        return None
    def updatedb(self, db):
        pass
    def finalize(self):
        pass
    def sanitize(self):
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
        self.filename = db.get_temp_filename(name, node)
    def get_outputs(self):
        yield pypeliner.resources.Dependency(self.name, self.node)
    def _remove(self):
        pypeliner.helpers.removefiledir(self.filename)
    def resolve(self):
        if self.cleanup in ('before', 'both'):
            self._remove()
        if self.node.undefined:
            raise Exception('Undefined node {} for {}'.format(self.node.displayname, self.name))
        return self.filename
    def finalize(self):
        if self.cleanup in ('after', 'both'):
            self._remove()


class MergeTemplateArg(Arg):
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
                self.formatted[node[-1][1]] = self.template.format(**dict(node))
            else:
                self.formatted[node[-1][1]] = self.name.format(**dict(node))
    def get_inputs(self):
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
        self.resource = pypeliner.resources.UserResource(name, node, filename)
    def get_inputs(self):
        yield self.resource
    def resolve(self):
        return self.resource.filename


class MergeFileArg(Arg,SplitMergeArg):
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
        self.inputs = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_user_filename(self.name, node, fnames=self.fnames, template=self.template)
            resource = pypeliner.resources.UserResource(self.name, node, filename)
            self.resources.append(resource)
            self.inputs.append(resource)
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            self.inputs.append(dependency)
    def get_inputs(self):
        return self.inputs
    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            resolved[self.get_node_chunks(resource.node)] = resource.filename
        return resolved


class OutputFileArg(Arg):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """
    def __init__(self, db, name, node, fnames=None, template=None, **kwargs):
        filename = db.get_user_filename(name, node, fnames=fnames, template=template)
        self.resource = pypeliner.resources.UserResource(name, node, filename, direct_write=kwargs['direct_write'])
    def get_outputs(self):
        yield self.resource
    def resolve(self):
        self.resolved = self.resource.write_filename
        pypeliner.helpers.makedirs(os.path.dirname(self.resolved))
        return self.resolved
    def finalize(self):
        self.resource.finalize()
    def updatedb(self, db):
        self.resource.update_fstats()


class SplitFileArg(Arg,SplitMergeArg):
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
        self.direct_write = kwargs['direct_write']
        self.resources = []
        self.outputs = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_user_filename(self.name, node, fnames=self.fnames, template=self.template)
            resource = pypeliner.resources.UserResource(self.name, node, filename, direct_write=kwargs['direct_write'])
            self.resources.append(resource)
            self.outputs.append(resource)
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin):
            self.outputs.append(dependency)
        self.merge_inputs = list(db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin))
        filename_creator = db.get_user_filename_creator(
            self.name, self.node.axes + self.axes, fnames=self.fnames, template=self.template)
        self.filename_callback = UserFilenameCallback(
            self.name, self.node, self.axes, self.direct_write, filename_creator)
    def get_inputs(self):
        return self.merge_inputs
    def get_outputs(self):
        return self.outputs
    def resolve(self):
        return self.filename_callback
    def updatedb(self, db):
        if self.is_split:
            db.nodemgr.store_chunks(self.axes, self.node, self.filename_callback.resources.keys(), subset=self.axes_origin)
        for resource in self.filename_callback.resources.itervalues():
            resource.update_fstats()
    def finalize(self):
        for resource in self.filename_callback.resources.itervalues():
            resource.finalize()


class TempInputObjArg(Arg):
    """ Temporary input object argument

    Resolves to an object.  If func is given, resolves to the return value of func called with object as the only
    parameter.

    """
    def __init__(self, db, name, node, func=None, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempObjManager(name, node, filename)
        self.func = func
    def get_inputs(self):
        yield self.resource.input
    def resolve(self):
        obj = self.resource.get_obj()
        if self.func is not None:
            obj = self.func(obj)
        return obj
    def sanitize(self):
        del self.func


class TempMergeObjArg(Arg,SplitMergeArg):
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
            resource = pypeliner.resources.TempObjManager(self.name, node, filename)
            self.resources.append(resource)
        self.merge_inputs = []
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            self.merge_inputs.append(dependency)
    def get_inputs(self):
        for resource in self.resources:
            yield resource.input
        for dependency in self.merge_inputs:
            yield dependency
    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            obj = resource.get_obj()
            if self.func is not None:
                obj = self.func(obj)
            resolved[self.get_node_chunks(resource.node)] = obj
        return resolved
    def sanitize(self):
        del self.func


class TempOutputObjArg(Arg):
    """ Temporary output object argument

    Stores an object created by a job.

    """
    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempObjManager(name, node, filename)
    def get_outputs(self):
        yield self.resource.output
    def resolve(self):
        return self
    def updatedb(self, db):
        self.resource.finalize(self.value)
        self.resource.update_fstats()


class TempSplitObjArg(Arg,SplitMergeArg):
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
            resource = pypeliner.resources.TempObjManager(self.name, node, filename)
            self.resources.append(resource)
        self.merge_inputs = []
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin):
            self.merge_inputs.append(dependency)
        self.split_outputs = []
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin):
            self.split_outputs.append(dependency)
    def get_inputs(self):
        return self.merge_inputs
    def get_outputs(self):
        for resource in self.resources:
            yield resource.output
        for dependency in self.split_outputs:
            yield dependency
    def resolve(self):
        return self
    def updatedb(self, db):
        db.nodemgr.store_chunks(self.axes, self.node, self.value.keys(), subset=self.axes_origin)
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            node_chunks = self.get_node_chunks(node)
            if node_chunks not in self.value:
                raise ValueError('unable to extract ' + str(node) + ' from ' + self.name + ' with values ' + str(self.value))
            instance_value = self.value[node_chunks]
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempObjManager(self.name, node, filename)
            resource.finalize(instance_value)
            resource.update_fstats()


class TempInputFileArg(Arg):
    """ Temp input file argument

    Resolves to a filename for a temporary file.

    """
    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempFileResource(name, node, filename)
    def get_inputs(self):
        yield self.resource
    def resolve(self):
        return self.resource.filename


class TempMergeFileArg(Arg,SplitMergeArg):
    """ Temp input files merged along a single axis

    Resolves to a dictionary of filenames of temporary files.

    """
    def __init__(self, db, name, node, axes, **kwargs):
        self.name = name
        self.node = node
        self.axes = axes
        self.resources = []
        self.inputs = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempFileResource(self.name, node, filename)
            self.resources.append(resource)
            self.inputs.append(resource)
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            self.inputs.append(dependency)
    def get_inputs(self):
        return self.inputs
    def resolve(self):
        resolved = dict()
        for resource in self.resources:
            resolved[self.get_node_chunks(resource.node)] = resource.filename
        return resolved


class TempOutputFileArg(Arg):
    """ Temp output file argument

    Resolves to an output filename for a temporary file.  Finalizes with resource manager.

    """
    def __init__(self, db, name, node, **kwargs):
        filename = db.get_temp_filename(name, node)
        self.resource = pypeliner.resources.TempFileResource(name, node, filename, direct_write=kwargs['direct_write'])
    def get_outputs(self):
        yield self.resource
    def resolve(self):
        self.resolved = self.resource.write_filename
        return self.resolved
    def finalize(self):
        self.resource.finalize()
    def updatedb(self, db):
        self.resource.update_fstats()


class FilenameCallback(object):
    """ Argument to split jobs providing callback for filenames
    with a particular instance """
    def __init__(self, name, node, axes, direct_write, filename_creator):
        self.name = name
        self.node = node
        self.axes = axes
        self.direct_write = direct_write
        self.filename_creator = filename_creator
        self.resources = dict()
    def __call__(self, *chunks):
        return self.__getitem__(*chunks)
    def __getitem__(self, *chunks):
        if len(self.axes) != len(chunks):
            raise ValueError('expected ' + str(len(self.axes)) + ' values for axes ' + str(self.axes))
        node = self.node
        for axis, chunk in zip(self.axes, chunks):
            node += pypeliner.identifiers.AxisInstance(axis, chunk)
        filename = self.filename_creator(self.name, node)
        resource = self.create_resource(self.name, node, filename, self.direct_write)
        if len(chunks) == 1:
            self.resources[chunks[0]] = resource
        else:
            self.resources[chunks] = resource
        pypeliner.helpers.makedirs(os.path.dirname(filename))
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


class TempSplitFileArg(Arg,SplitMergeArg):
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
        self.direct_write = kwargs['direct_write']
        self.resources = []
        self.inputs = []
        self.outputs = []
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            filename = db.get_temp_filename(self.name, node)
            resource = pypeliner.resources.TempFileResource(self.name, node, filename, direct_write=kwargs['direct_write'])
            self.resources.append(resource)
            self.outputs.append(resource)
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin):
            self.inputs.append(dependency)
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin):
            self.outputs.append(dependency)
        self.filename_callback = TempFilenameCallback(
            self.name, self.node, self.axes, self.direct_write,
            db.get_temp_filename_creator())
    def get_inputs(self):
        return self.inputs
    def get_outputs(self):
        return self.outputs
    def resolve(self):
        return self.filename_callback
    def updatedb(self, db):
        if self.is_split:
            db.nodemgr.store_chunks(self.axes, self.node, self.filename_callback.resources.keys(), subset=self.axes_origin)
        for resource in self.filename_callback.resources.itervalues():
            resource.update_fstats()
    def finalize(self):
        for resource in self.filename_callback.resources.itervalues():
            resource.finalize()


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
        self.inputs = []
        for dependency in db.nodemgr.get_merge_inputs(self.axis, self.node):
            self.inputs.append(dependency)
        self.chunks = list(db.nodemgr.retrieve_chunks(self.axis, self.node))
        if len(self.axis) == 1:
            self.chunks = [chunk[0] for chunk in self.chunks]
    def get_inputs(self):
        return self.inputs
    def resolve(self):
        return self.chunks


class OutputChunksArg(Arg,SplitMergeArg):
    """ Instance list of a job as an argument

    Sets the list of chunks for the given axes.

    """
    def __init__(self, db, name, node, axes, axes_origin=None, **kwargs):
        self.node = node
        self.axes = axes
        self.axes_origin = self.get_axes_origin(axes_origin)
        self.is_split = len(self.axes_origin) != 0
        self.inputs = []
        self.outputs = []
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node, subset=self.axes_origin):
            self.inputs.append(dependency)
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node, subset=self.axes_origin):
            self.outputs.append(dependency)
    def get_inputs(self):
        return self.inputs
    def get_outputs(self):
        return self.outputs
    def resolve(self):
        return self
    def updatedb(self, db):
        db.nodemgr.store_chunks(self.axes, self.node, self.value, subset=self.axes_origin)

