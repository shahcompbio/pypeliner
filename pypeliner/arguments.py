import os

import helpers
import resources
import resourcemgr

class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename
    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)

def resolve_arg(arg):
    """ Resolve an Arg object into a concrete argument """
    if isinstance(arg, Arg):
        return arg.resolve()
    else:
        return arg


class Arg(object):
    @property
    def inputs(self):
        return []
    @property
    def outputs(self):
        return []
    @property
    def is_split(self):
        return False
    def resolve(self):
        return None
    def finalize(self, resolved):
        pass


class TemplateArg(Arg):
    """ Templated name argument 

    The name parameter is treated as a string with named formatting.  Resolves to the name formatted using the node 
    dictionary.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = name.format(**dict(node))
    def resolve(self):
        return self.filename


class TempFileArg(Arg):
    """ Temporary file argument

    Resolves to a filename contained within the temporary files directory.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = resmgr.get_final_filename(name, node)
    def resolve(self):
        return self.filename


class MergeTemplateArg(Arg):
    """ Temp input files merged along a single axis

    The name parameter is treated as a string with named formatting.  Resolves to a dictionary with the keys as chunks
    for the merge axis.  Each value is the name formatted using the merge node dictionary.

    """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resolved[node[-1][1]] = self.name.format(**dict(node))
        return resolved


class UserFilenameCreator(object):
    """ Function object for creating user filenames from name node pairs """
    def __init__(self, suffix=''):
        self.suffix = suffix
    def __call__(self, name, node):
        return name.format(**dict(node)) + self.suffix
    def __repr__(self):
        return '{0}.{1}({2})'.format(resourcemgr.FilenameCreator.__module__, resourcemgr.FilenameCreator.__name__, self.suffix)


class InputFileArg(Arg):
    """ Input file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def inputs(self):
        yield resources.UserResource(self.resolve())
    def resolve(self):
        return self.name.format(**dict(self.node))


class MergeFileArg(Arg):
    """ Input files merged along a single axis

    The name argument is treated as a filename with named formatting.  Resolves to a dictionary with keys as chunks for
    the merge axis.  Each value is the filename formatted using the merge node dictonary.

    """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def inputs(self):
        for filename in self.resolve().itervalues():
            yield resources.UserResource(filename)
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resolved[node[-1][1]] = self.name.format(**dict(node))
        return resolved


class OutputFileArg(Arg):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = self.name.format(**dict(node))
        self.temp_filename = self.filename + '.tmp'
    @property
    def outputs(self):
        yield resources.UserResource(self.filename)
    def resolve(self):
        return self.temp_filename
    def finalize(self, resolved):
        try:
            os.rename(self.temp_filename, self.filename)
        except OSError:
            raise OutputMissingException(self.temp_filename)


class SplitFileArg(Arg):
    """ Output file arguments from a split

    The name argument is treated as a filename with named formatting.  Resolves to a filename callback that can be used
    to generate filenames based on a given split axis chunk.  Resolved filenames have the '.tmp' suffix.  Finalizing
    involves removing the '.tmp' suffix for each file created by the job.

    """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def filenames(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.name.format(**dict(node))
    @property
    def outputs(self):
        for filename in self.filenames:
            yield resources.UserResource(filename)
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def resolve(self):
        return FilenameCallback(self.name, self.base_node, self.split_axis, UserFilenameCreator('.tmp'))
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.chunks)
        for filename in self.filenames:
            temp_filename = filename + '.tmp'
            try:
                os.rename(temp_filename, filename)
            except OSError:
                raise OutputMissingException(temp_filename)


class TempInputObjArg(Arg):
    """ Temporary input object argument

    Resolves to an object.

    """
    def __init__(self, resmgr, nodemgr, name, node, func=None):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.func = func
    @property
    def inputs(self):
        yield self.resmgr.create_input_object_resource(self.name, self.node)
    def resolve(self):
        obj = self.resmgr.retrieve_object(self.name, self.node)
        if self.func is not None:
            obj = self.func(obj)
        return obj


class TempMergeObjArg(Arg):
    """ Temp input object arguments merged along single axis

    Resolves to an dictionary of objects with keys given by the merge axis chunks.

    """
    def __init__(self, resmgr, nodemgr, name, node, merge_axis, func=None):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = node
        self.merge_axis = merge_axis
        self.func = func
    @property
    def inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield self.resmgr.create_input_object_resource(self.name, node)
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            obj = self.resmgr.retrieve_object(self.name, node)
            if self.func is not None:
                obj = self.func(obj)
            resolved[node[-1][1]] = obj
        return resolved


class TempOutputObjArg(Arg):
    """ Temporary output object argument

    Stores an object created by a job.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def outputs(self):
        yield self.resmgr.create_output_file_resource(self.name, self.node)
    def finalize(self, resolved):
        self.resmgr.finalize_object(self.name, self.node, resolved)


class TempSplitObjArg(Arg):
    """ Temporary output object arguments from a split

    Stores a dictionary of objects created by a job.  The keys of the dictionary are taken as the chunks for the given
    split axis.

    """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.resmgr.create_output_file_resource(self.name, node)
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.keys())
        for chunk, obj in resolved.iteritems():
            node = self.base_node + ((self.split_axis, chunk),)
            self.resmgr.finalize_object(self.name, node, obj)


class TempInputFileArg(Arg):
    """ Temp input file argument

    Resolves to a filename for a temporary file.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.resource = self.resmgr.create_input_file_resource(self.name, self.node)
    @property
    def inputs(self):
        yield self.resource
    def resolve(self):
        return self.resource.filename


class TempMergeFileArg(Arg):
    """ Temp input files merged along a single axis

    Resolves to a dictionary of filenames of temporary files.

    """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def node_resources(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield node, self.resmgr.create_input_file_resource(self.name, node)
    @property
    def inputs(self):
        for node, resource in self.node_resources:
            yield resource
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node, resource in self.node_resources:
            resolved[node[-1][1]] = resource.filename
        return resolved


class TempOutputFileArg(Arg):
    """ Temp output file argument

    Resolves to an output filename for a temporary file.  Finalizes with resource manager.

    """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.resource = self.resmgr.create_output_file_resource(self.name, self.node)
    @property
    def outputs(self):
        yield self.resource
    def resolve(self):
        return self.resource.filename
    def finalize(self, resolved):
        self.resmgr.finalize_output(self.resource)


class FilenameCallback(object):
    """ Argument to split jobs providing callback for filenames
    with a particular instance """
    def __init__(self, name, base_node, split_axis, filename_creator):
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
        self.filename_creator = filename_creator
        self.chunks = set()
    def __call__(self, chunk):
        self.chunks.add(chunk)
        filename = self.filename_creator(self.name, self.base_node + ((self.split_axis, chunk),))
        helpers.makedirs(os.path.dirname(filename))
        return filename
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCallback.__module__, FilenameCallback.__name__, ', '.join(repr(a) for a in (self.name, self.base_node, self.split_axis, self.filename_creator)))


class TempSplitFileArg(Arg):
    """ Temp output file arguments from a split

    Resolves to a filename callback that can be used to create a temporary filename for each chunk of the split on the 
    given axis.  Finalizes with resource manager to move from temporary filename to final filename.

    """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def resources(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.resmgr.create_output_file_resource(self.name, node)
    @property
    def outputs(self):
        for resource in self.resources:
            yield resource
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def resolve(self):
        return FilenameCallback(self.name, self.base_node, self.split_axis, self.resmgr.filename_creator)
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.chunks)
        for resource in self.resources:
            self.resmgr.finalize_output(resource)


class InputInstanceArg(Arg):
    """ Instance of a job as an argument

    Resolves to the instance of the given job for a specific axis.

    """
    def __init__(self, resmgr, nodemgr, node, axis):
        self.chunk = dict(node)[axis]
    def resolve(self):
        return self.chunk
    def finalize(self, resolved):
        pass

class InputChunksArg(Arg):
    """ Instance list of an axis as an argument

    Resolves to the list of chunks for the given axes.

    """
    def __init__(self, resmgr, nodemgr, name, node, axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.node = node
        self.axis = axis
    @property
    def inputs(self):
        yield self.nodemgr.get_input(self.axis, self.node)
    def resolve(self):
        return self.nodemgr.retrieve_chunks(self.axis, self.node)
    def finalize(self, resolved):
        pass

class OutputChunksArg(Arg):
    """ Instance list of a job as an argument

    Sets the list of chunks for the given axes.

    """
    def __init__(self, resmgr, nodemgr, name, node, axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.node = node
        self.axis = axis
    @property
    def outputs(self):
        yield self.nodemgr.get_output(self.axis, self.node)
    @property
    def is_split(self):
        return True
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.axis, self.node, resolved)

