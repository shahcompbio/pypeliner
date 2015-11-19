import os
import copy

import helpers
import resources
import identifiers


class Arg(object):
    def get_inputs(self, db):
        return []
    def get_outputs(self, db):
        return []
    @property
    def is_split(self):
        return False
    def resolve(self, db):
        return None
    def updatedb(self, db):
        pass
    def finalize(self, db):
        pass
    def sanitize(self):
        pass
    def __deepcopy__(self, memo):
        arg = copy.copy(self)
        resolved = arg.resolve(memo['_db'])
        arg.sanitize()
        memo['_args'].append(arg)
        return resolved


class TemplateArg(Arg):
    """ Templated name argument 

    The name parameter is treated as a string with named formatting.  Resolves to the name formatted using the node 
    dictionary.

    """
    def __init__(self, db, name, node, template=None):
        self.name = name
        self.node = node
        if template is not None:
            self.filename = template.format(**dict(node))
        else:
            self.filename = name.format(**dict(node))
    def resolve(self, db):
        return self.filename


class TempFileArg(Arg):
    """ Temporary file argument

    Resolves to a filename contained within the temporary files directory.

    """
    def __init__(self, db, name, node):
        self.name = name
        self.node = node
    def resolve(self, db):
        return db.resmgr.get_filename(self.name, self.node)


class MergeTemplateArg(Arg):
    """ Temp input files merged along a single axis

    The name parameter is treated as a string with named formatting.  Resolves to a dictionary with the keys as chunks
    for the merge axis.  Each value is the name formatted using the merge node dictionary.

    """
    def __init__(self, db, name, node, axes, template=None):
        self.name = name
        self.node = node
        self.axes = axes
        self.template = template
    @property
    def resolve(self, db):
        resolved = dict()
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            if self.template is not None:
                resolved[node[-1][1]] = self.template.format(**dict(node))
            else:
                resolved[node[-1][1]] = self.name.format(**dict(node))
        return resolved


class UserFilenameCreator(object):
    """ Function object for creating user filenames from name node pairs """
    def __init__(self, suffix='', fnames=None, template=None):
        self.suffix = suffix
        self.fnames = fnames
        self.template = template
    def __call__(self, name, node):
        resource = resources.UserResource(name, node, fnames=self.fnames, template=self.template)
        return resource.filename + self.suffix
    def __repr__(self):
        return '{0}.{1}({2})'.format(resourcemgr.FilenameCreator.__module__, resourcemgr.FilenameCreator.__name__, self.suffix)


class InputFileArg(Arg):
    """ Input file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary.

    """
    def __init__(self, db, name, node, fnames=None, template=None):
        self.resource = resources.UserResource(name, node, fnames=fnames, template=template)
    def get_inputs(self, db):
        yield self.resource
    def resolve(self, db):
        return self.resource.filename


class MergeFileArg(Arg):
    """ Input files merged along a single axis

    The name argument is treated as a filename with named formatting.  Resolves to a dictionary with keys as chunks for
    the merge axis.  Each value is the filename formatted using the merge node dictonary.

    """
    def __init__(self, db, name, node, axes, fnames=None, template=None):
        self.name = name
        self.node = node
        self.axes = axes
        self.fnames = fnames
        self.template = template
    def get_resources(self, db):
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            yield resources.UserResource(self.name, node, fnames=self.fnames, template=self.template)
    def get_inputs(self, db):
        for resource in self.get_resources(db):
            yield resource
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            yield dependency
    def resolve(self, db):
        resolved = dict()
        for resource in self.get_resources(db):
            chunks = tuple(a[1] for a in resource.node[-len(self.axes):])
            resolved[chunks] = resource.filename
        return resolved


class OutputFileArg(Arg):
    """ Output file argument

    The name argument is treated as a filename with named formatting.  Resolves to a filename formatted using the node
    dictionary, including the '.tmp' suffix.

    """
    def __init__(self, db, name, node, fnames=None, template=None):
        self.resource = resources.UserResource(name, node, fnames=fnames, template=template)
    def get_outputs(self, db):
        yield self.resource
    def resolve(self, db):
        self.resolved = self.resource.filename + '.tmp'
        return self.resolved
    def finalize(self, db):
        self.resource.finalize(self.resolved, db)


class SplitFileArg(Arg):
    """ Output file arguments from a split

    The name argument is treated as a filename with named formatting.  Resolves to a filename callback that can be used
    to generate filenames based on a given split axis chunk.  Resolved filenames have the '.tmp' suffix.  Finalizing
    involves removing the '.tmp' suffix for each file created by the job.

    """
    def __init__(self, db, name, node, axes, fnames=None, template=None):
        self.name = name
        self.node = node
        self.axes = axes
        self.fnames = fnames
        self.template = template
    def get_resources(self, db):
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            yield resources.UserResource(self.name, node, fnames=self.fnames, template=self.template)
    def get_outputs(self, db):
        for resource in self.get_resources(db):
            yield resource
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node):
            yield dependency
    @property
    def is_split(self):
        return True
    def resolve(self, db):
        self.resolved = FilenameCallback(self, UserFilenameCreator('.tmp', self.fnames, self.template))
        return self.resolved
    def updatedb(self, db):
        self.resolved.updatedb(db)
    def finalize(self, db):
        self.resolved.finalize(db)


class TempInputObjArg(Arg):
    """ Temporary input object argument

    Resolves to an object.  If func is given, resolves to the return value of func called with object as the only
    parameter.

    """
    def __init__(self, db, name, node, func=None):
        self.resource = resources.TempObjManager(name, node)
        self.func = func
    def get_inputs(self, db):
        yield self.resource.input
    def resolve(self, db):
        obj = self.resource.get_obj(db)
        if self.func is not None:
            obj = self.func(obj)
        return obj
    def sanitize(self):
        del self.func


class TempMergeObjArg(Arg):
    """ Temp input object arguments merged along single axis

    Resolves to an dictionary of objects with keys given by the merge axis chunks.

    """
    def __init__(self, db, name, node, axes, func=None):
        self.name = name
        self.node = node
        self.axes = axes
        self.func = func
    def get_resources(self, db):
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
             yield resources.TempObjManager(self.name, node)
    def get_inputs(self, db):
        for resource in self.get_resources(db):
            yield resource.input
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            yield dependency
    def resolve(self, db):
        resolved = dict()
        for resource in self.get_resources(db):
            obj = resource.get_obj(db)
            if self.func is not None:
                obj = self.func(obj)
            chunks = tuple(a[1] for a in resource.node[-len(self.axes):])
            resolved[resource.chunk] = obj
        return resolved
    def sanitize(self):
        del self.func


class TempOutputObjArg(Arg):
    """ Temporary output object argument

    Stores an object created by a job.

    """
    def __init__(self, db, name, node):
        self.resource = resources.TempObjManager(name, node)
    def get_outputs(self, db):
        yield self.resource.output
    def resolve(self, db):
        return self
    def finalize(self, db):
        self.resource.finalize(self.value, db)


class TempSplitObjArg(Arg):
    """ Temporary output object arguments from a split

    Stores a dictionary of objects created by a job.  The keys of the dictionary are taken as the chunks for the given
    split axis.

    """
    def __init__(self, db, name, node, axes):
        self.name = name
        self.node = node
        self.axes = axes
    def get_resources(self, db):
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
             yield resources.TempObjManager(self.name, node)
    def get_outputs(self, db):
        for resource in self.get_resources(db):
            yield resource.output
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node):
            yield dependency
    @property
    def is_split(self):
        return True
    def resolve(self, db):
        return self
    def updatedb(self, db):
        db.nodemgr.store_chunks(self.axes, self.node, self.value.keys())
    def finalize(self, db):
        for resource in self.get_resources(db):
            chunks = tuple(a[1] for a in resource.node[-len(self.axes):])
            instance_value = None
            try:
                instance_value = self.value[chunks]
            except KeyError:
                pass
            try:
                if len(chunks) == 1:
                    instance_value = self.value[chunks[0]]
            except KeyError:
                pass
            if instance_value is None:
                raise ValueError('unable to extract ' + chunks + ' from ' + self.name)
            resource.finalize(instance_value, db)


class TempInputFileArg(Arg):
    """ Temp input file argument

    Resolves to a filename for a temporary file.

    """
    def __init__(self, db, name, node):
        self.resource = resources.TempFileResource(name, node, db)
    def get_inputs(self, db):
        yield self.resource
    def resolve(self, db):
        return self.resource.get_filename(db)


class TempMergeFileArg(Arg):
    """ Temp input files merged along a single axis

    Resolves to a dictionary of filenames of temporary files.

    """
    def __init__(self, db, name, node, axes):
        self.name = name
        self.node = node
        self.axes = axes
    def get_resources(self, db):
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            yield resources.TempFileResource(self.name, node, db)
    def get_inputs(self, db):
        for resource in self.get_resources(db):
            yield resource
        for dependency in db.nodemgr.get_merge_inputs(self.axes, self.node):
            yield dependency
    def resolve(self, db):
        resolved = dict()
        for resource in self.get_resources(db):
            chunks = tuple(a[1] for a in resource.node[-len(self.axes):])
            resolved[chunks] = resource.get_filename(db)
        return resolved


class TempOutputFileArg(Arg):
    """ Temp output file argument

    Resolves to an output filename for a temporary file.  Finalizes with resource manager.

    """
    def __init__(self, db, name, node):
        self.resource = resources.TempFileResource(name, node, db)
    def get_outputs(self, db):
        yield self.resource
    def resolve(self, db):
        self.resolved = self.resource.get_filename(db) + '.tmp'
        return self.resolved
    def finalize(self, db):
        self.resource.finalize(self.resolved, db)


class FilenameCallback(object):
    """ Argument to split jobs providing callback for filenames
    with a particular instance """
    def __init__(self, arg, filename_creator):
        self.arg = arg
        self.filename_creator = filename_creator
        self.filenames = dict()
    def __call__(self, *chunks):
        if len(self.arg.axes) != len(chunks):
            raise ValueError('expected ' + str(len(self.arg.axes)) + ' values for axes ' + str(self.arg.axes))
        node = self.arg.node
        for axis, chunk in zip(self.arg.axes, chunks):
            node += identifiers.AxisInstance(axis, chunk)
        filename = self.filename_creator(self.arg.name, node)
        self.filenames[chunks] = filename
        helpers.makedirs(os.path.dirname(filename))
        return filename
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCallback.__module__, FilenameCallback.__name__, ', '.join(repr(a) for a in (self.arg.name, self.arg.node, self.arg.axes, self.filename_creator)))
    def updatedb(self, db):
        db.nodemgr.store_chunks(self.arg.axes, self.arg.node, self.filenames.keys())
    def finalize(self, db):
        for resource in self.arg.get_resources(db):
            chunks = tuple(a[1] for a in resource.node[-len(self.arg.axes):])
            resource.finalize(self.filenames[chunks], db)


class TempSplitFileArg(Arg):
    """ Temp output file arguments from a split

    Resolves to a filename callback that can be used to create a temporary filename for each chunk of the split on the 
    given axis.  Finalizes with resource manager to move from temporary filename to final filename.

    """
    def __init__(self, db, name, node, axes):
        self.name = name
        self.node = node
        self.axes = axes
    def get_resources(self, db):
        for node in db.nodemgr.retrieve_nodes(self.axes, self.node):
            yield resources.TempFileResource(self.name, node, db)
    def get_outputs(self, db):
        for resource in self.get_resources(db):
            yield resource
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node):
            yield dependency
    @property
    def is_split(self):
        return True
    def resolve(self, db):
        self.resolved = FilenameCallback(self, db.resmgr.filename_creator)
        return self.resolved
    def updatedb(self, db):
        self.resolved.updatedb(db)
    def finalize(self, db):
        self.resolved.finalize(db)


class InputInstanceArg(Arg):
    """ Instance of a job as an argument

    Resolves to the instance of the given job for a specific axis.

    """
    def __init__(self, db, node, axis):
        self.chunk = dict(node)[axis]
    def resolve(self, db):
        return self.chunk


class InputChunksArg(Arg):
    """ Instance list of an axis as an argument

    Resolves to the list of chunks for the given axes.

    """
    def __init__(self, db, name, node, axis):
        self.node = node
        self.axis = axis
    def get_inputs(self, db):
        for dependency in db.nodemgr.get_merge_inputs(self.axis, self.node):
            yield dependency
    def resolve(self, db):
        return list(db.nodemgr.retrieve_chunks(self.axis, self.node))


class OutputChunksArg(Arg):
    """ Instance list of a job as an argument

    Sets the list of chunks for the given axes.

    """
    def __init__(self, db, name, node, axes):
        self.node = node
        self.axes = axes
    def get_outputs(self, db):
        for dependency in db.nodemgr.get_split_outputs(self.axes, self.node):
            yield dependency
    @property
    def is_split(self):
        return True
    def resolve(self, db):
        return self
    def finalize(self, db):
        db.nodemgr.store_chunks(self.axes, self.node, self.value)

