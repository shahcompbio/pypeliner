import arguments

class JobArgMismatchException(Exception):
    def __init__(self, name, axes, node):
        self.name = name
        self.axes = axes
        self.node = node
        self.job_name = 'unknown'
    def __str__(self):
        return 'arg {0} with axes {1} does not match job {2} with axes {3}'.format(self.name, self.axes, self.job_name, tuple(a[0] for a in self.node))

def create_arg(resmgr, nodemgr, mgd, node):
    """ Translate a managed user argument into an internally used Arg object """
    if isinstance(mgd, Managed):
        return mgd.create_arg(resmgr, nodemgr, node)
    else:
        return mgd

class Managed(object):
    """ Interface class used to represent a managed data """
    def __init__(self, name, *axes):
        if name is not None and type(name) != str:
            raise ValueError('name of argument must be string')
        if type(axes) != tuple:
            raise ValueError('axes must be a tuple')
        self.name = name
        self.axes = axes
    def _create_arg(self, resmgr, nodemgr, node, normal=None, splitmerge=None, **kwargs):
        common = 0
        for node_axis, axis in zip((a[0] for a in node), self.axes):
            if node_axis != axis:
                break
            common += 1
        axes_specific = self.axes[common:]
        if len(axes_specific) == 0 and normal is not None:
            return normal(resmgr, nodemgr, self.name, node[:common], **kwargs)
        elif len(axes_specific) == 1 and splitmerge is not None:
            return splitmerge(resmgr, nodemgr, self.name, node[:common], axes_specific[0], **kwargs)
        else:
            raise JobArgMismatchException(self.name, self.axes, node)

class Template(Managed):
    """ Represents a name templated by axes """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TemplateArg, splitmerge=arguments.MergeTemplateArg)

class TempFile(Managed):
    """ Represents a temp file the can be written to but is not a dependency """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempFileArg)

class InputFile(Managed):
    """ Interface class used to represent a user specified managed input file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.InputFileArg, splitmerge=arguments.MergeFileArg)

class OutputFile(Managed):
    """ Interface class used to represent a user specified managed output file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.OutputFileArg, splitmerge=arguments.SplitFileArg)

class TempInputObj(Managed):
    """ Interface class used to represent a managed input object """
    def prop(self, prop_name):
        return TempInputObjExtract(self.name, self.axes, lambda a: getattr(a, prop_name))
    def extract(self, func):
        return TempInputObjExtract(self.name, self.axes, func)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempInputObjArg, splitmerge=arguments.TempMergeObjArg)

class TempOutputObj(Managed):
    """ Interface class used to represent a managed output object """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempOutputObjArg, splitmerge=arguments.TempSplitObjArg)

class TempInputObjExtract(Managed):
    """ Interface class used to represent a property of a managed
    input object """
    def __init__(self, name, axes, func):
        Managed.__init__(self, name, *axes)
        self.func = func
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempInputObjArg, splitmerge=arguments.TempMergeObjArg, func=self.func)

class TempInputFile(Managed):
    """ Interface class used to represent a managed input file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempInputFileArg, splitmerge=arguments.TempMergeFileArg)

class TempOutputFile(Managed):
    """ Interface class used to represent a managed input file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=arguments.TempOutputFileArg, splitmerge=arguments.TempSplitFileArg)

class Instance(Managed):
    """ Interface class used to represent the instance of a job as
    an input parameter """
    def __init__(self, axis):
        self.axis = axis
    def create_arg(self, resmgr, nodemgr, node):
        return arguments.InputInstanceArg(resmgr, nodemgr, node, self.axis)

class InputChunks(Managed):
    """ Interface class used to represent the list of chunks
    from a split along a specific axis """
    def __init__(self, *axes):
        Managed.__init__(self, None, *axes)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=None, splitmerge=arguments.InputChunksArg)

class OutputChunks(Managed):
    """ Interface class used to represent the list of chunks
    from a split along a specific axis """
    def __init__(self, *axes):
        Managed.__init__(self, None, *axes)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=None, splitmerge=arguments.OutputChunksArg)

