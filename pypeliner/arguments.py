import os
import pickle

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
    """ Templated name argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = user_filename_creator(name, node)
    def resolve(self):
        return self.filename

class TempFileArg(Arg):
    """ Temporary file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    def resolve(self):
        return self.resmgr.get_output(self.name, self.node).final_filename

class MergeTemplateArg(Arg):
    """ Temp input files merged along a single axis """
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
            resource = resources.UserResource(user_filename_creator(self.name, node))
            resolved[node[-1][1]] = resource.filename
        return resolved

class UserFilenameCreator(object):
    """ Function object for creating user filenames from name node pairs """
    def __init__(self, suffix=''):
        self.suffix = suffix
    def __call__(self, name, node):
        return name.format(**dict(node)) + self.suffix
    def __repr__(self):
        return '{0}.{1}({2})'.format(resourcemgr.FilenameCreator.__module__, resourcemgr.FilenameCreator.__name__, self.suffix)

user_filename_creator = UserFilenameCreator()

class InputFileArg(Arg):
    """ Input file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = user_filename_creator(name, node)
    @property
    def inputs(self):
        yield resources.UserResource(self.filename)
    def resolve(self):
        return self.filename

class MergeFileArg(Arg):
    """ Temp input files merged along a single axis """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield resources.UserResource(user_filename_creator(self.name, node))
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = resources.UserResource(user_filename_creator(self.name, node))
            resolved[node[-1][1]] = resource.filename
        return resolved

class OutputFileArg(Arg):
    """ Input file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = user_filename_creator(name, node)
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
    """ Output file arguments from a split """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield resources.UserResource(user_filename_creator(self.name, node))
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def resolve(self):
        return FilenameCallback(self.name, self.base_node, self.split_axis, UserFilenameCreator('.tmp'))
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.chunks)
        for chunk in resolved.chunks:
            node = self.base_node + ((self.split_axis, chunk),)
            filename = user_filename_creator(self.name, node)
            temp_filename = filename + '.tmp'
            try:
                os.rename(temp_filename, filename)
            except OSError:
                raise OutputMissingException(temp_filename)

class TempInputObjArg(Arg):
    """ Temp input object argument """
    def __init__(self, resmgr, nodemgr, name, node, func=None):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.func = func
    @property
    def inputs(self):
        yield self.resmgr.get_input(self.name, self.node)
    def resolve(self):
        resource = self.resmgr.get_input(self.name, self.node)
        with open(resource.filename, 'rb') as f:
            obj = pickle.load(f)
            if self.func is not None:
                obj = self.func(obj)
            return obj

class TempMergeObjArg(Arg):
    """ Temp input object arguments merged along single axis """
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
            yield self.resmgr.get_input(self.name, node)
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = self.resmgr.get_input(self.name, node)
            with open(resource.filename, 'rb') as f:
                obj = pickle.load(f)
                if self.func is not None:
                    obj = self.func(obj)
                resolved[node[-1][1]] = obj
        return resolved

class TempOutputObjArg(Arg):
    """ Temp output object argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def outputs(self):
        yield self.resmgr.get_output(self.name, self.node)
    def finalize(self, resolved):
        resource = self.resmgr.get_output(self.name, self.node)
        with open(resource.filename, 'wb') as f:
            pickle.dump(resolved, f)
        self.resmgr.finalize_output(resource)

class TempSplitObjArg(Arg):
    """ Temp output object arguments from a split """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.resmgr.get_output(self.name, node)
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.keys())
        for chunk, object in resolved.iteritems():
            node = self.base_node + ((self.split_axis, chunk),)
            resource = self.resmgr.get_output(self.name, node)
            with open(resource.filename, 'wb') as f:
                pickle.dump(object, f)
            self.resmgr.finalize_output(resource)

class TempInputFileArg(Arg):
    """ Temp input file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def inputs(self):
        yield self.resmgr.get_input(self.name, self.node)
    def resolve(self):
        resource = self.resmgr.get_input(self.name, self.node)
        return resource.filename

class TempMergeFileArg(Arg):
    """ Temp input files merged along a single axis """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield self.resmgr.get_input(self.name, node)
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = self.resmgr.get_input(self.name, node)
            resolved[node[-1][1]] = resource.filename
        return resolved

class TempOutputFileArg(Arg):
    """ Temp output file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def outputs(self):
        yield self.resmgr.get_output(self.name, self.node)
    def resolve(self):
        return self.resmgr.get_output(self.name, self.node).filename
    def finalize(self, resolved):
        resource = self.resmgr.get_output(self.name, self.node)
        self.resmgr.finalize_output(resource)

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
    """ Temp output file arguments from a split """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.resmgr.get_output(self.name, node)
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def resolve(self):
        return FilenameCallback(self.name, self.base_node, self.split_axis, self.resmgr.filename_creator)
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.chunks)
        for chunk in resolved.chunks:
            resource = self.resmgr.get_output(self.name, self.base_node + ((self.split_axis, chunk),))
            self.resmgr.finalize_output(resource)

class InputInstanceArg(Arg):
    """ Instance of a job as an argument """
    def __init__(self, resmgr, nodemgr, node, axis):
        self.chunk = dict(node)[axis]
    def resolve(self):
        return self.chunk
    def finalize(self, resolved):
        pass

class InputChunksArg(Arg):
    """ Instance list of an axis as an argument """
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
    """ Instance list of a job as an argument """
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

