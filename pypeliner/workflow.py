import pypeliner.commandline
import pypeliner.jobs


class UserPathInfo(object):
    def __init__(self, template=None, fnames=None):
        self.template = template
        self.fnames = fnames

    def __eq__(self, other):
        return self.template == other.template and self.fnames == other.fnames

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '{0}.{1}({2}, {3})'.format(
            UserPathInfo.__module__,
            UserPathInfo.__name__,
            repr(self.template),
            repr(self.fnames))


parent_ctx = None


class Workflow(object):
    """ Contaner for a set of jobs making up a single workflow.

    """

    def __init__(self, ctx=None, default_ctx=None, default_sandbox=None):
        self.ctx = {
            'mem': 4,
            'num_retry': 3,
            'mem_retry_factor': 2,
            'ncpus': 1,
            'disk': 8
        }
        if parent_ctx is not None:
            self.ctx.update(parent_ctx)
        if ctx is not None:
            self.ctx.update(ctx)
        ## TODO: remove default_ctx at some point?
        if default_ctx is not None:
            DeprecationWarning("default_ctx will be replaced by ctx starting with v0.6")
            self.ctx.update(default_ctx)
        self.default_sandbox = default_sandbox
        self.job_definitions = dict()
        self.path_info = dict()

    @property
    def empty(self):
        return len(self.job_definitions) == 0

    def set_filenames(self, name, *axes, **kwargs):
        """ Set the filename for a
        """
        user_file_id = (name, axes)
        if user_file_id in self.path_info:
            raise ValueError(
                'Filename for {} with axes {} already set to {}'.format(name, axes, self.path_info[user_file_id])
            )
        self.path_info[user_file_id] = UserPathInfo()
        if 'fnames' in kwargs:
            self.path_info[user_file_id].fnames = kwargs['fnames']
        elif 'template' in kwargs:
            self.path_info[user_file_id].template = kwargs['template']
        elif 'filename' in kwargs:
            self.path_info[user_file_id].template = kwargs['filename']
        else:
            raise ValueError('One of fnames, template, or filename must be set')

    def setobj(self, obj=None, value=None, axes=()):
        """ Set a managed temp object with a specified value.

        :param obj: managed object to be set with a given value
        :type obj: :py:class:`pypeliner.managed.TempOutputObj`
        :param value: value to set
        :param axes: axes on which to perform operation.  If the axes argument is identical
                     to the axes of the object, the setting of the object occurs once per
                     axis chunk.  If the axes argument has length one less than the axes of
                     of obj, this setobj is a split operation that defines the additional
                     axis in obj.
        :type axes: tuple

        This function is most useful for tracking changes to small objects and parameters.
        Set the object to a given value using this function.  Then use the managed version
        of the object in calls to transform, and pypeliner will only trigger a rerun if the
        value of the object has changed in a subsequent run.

        """
        job_ctx = self.ctx.copy()
        job_ctx['local'] = True
        name = '_'.join(('setobj', str(obj.name)) + obj.axes)
        if name in self.job_definitions:
            raise ValueError('Object {} axes {} already set'.format(obj.name, repr(obj.axes)))

        self.job_definitions[name] = pypeliner.jobs.SetObjDefinition(name, axes, job_ctx, obj, value)

    def commandline(self, name='', axes=(), ctx=None, args=None, kwargs=None, sandbox=None):
        """ Add a command line based transform to the pipeline

        This call is equivalent to::

            self.transform(name, axes, ctx, commandline.execute, None, *args)

        See :py:func:`pypeliner.scheduler.transform`

        """
        if ctx is None:
            ctx = {}

        self.transform(
            name=name, axes=axes, ctx=ctx, func=pypeliner.commandline.execute,
            args=args, kwargs=kwargs, sandbox=None
        )

    def transform(self, name='', axes=(), ctx=None, func=None, ret=None, args=None, kwargs=None, sandbox=None):
        """ Add a transform to the pipeline.  A transform defines a job that uses the
        provided python function ``func`` to take input dependencies and create/update
        output dependents.

        :param name: unique name of the job, used to identify the job in logs and when
                     submitting instances to the exec queue
        :param axes: axes of the job.  defines the axes on which the job will operate.  A
                     job with an empty list for the axes has a single instance.  A job with
                     one axis in the axes list will have as many instances as were defined
                     for that axis by the split that is responsible for that axis.
        :param ctx: context of the job as a dictionary of key, value pairs.  The context
                    is given to the exec queue and provides a way of communicating jobs
                    specific requirements such as memory and cpu usage.  Setting
                    ``ctx['local'] = True`` will result in the job being run locally on
                    the calling machine even when a cluster is being used.
        :param func: The function to call for this job.
        :param ret: The return value
        :param args: The list of positional arguments to be used for the function call.
        :param kwargs: The list of keyword arguments to be used for the function call.

        Any value in args or kwargs that is an instance of
        :py:class:`pypeliner.managed.Managed` will be resolved to a pipeline managed
        file or object at runtime.  See :py:mod:`pypeliner.managed`.

        Acceptable values given for ``ret`` are restricted to a subset of
        :py:class:`pypeliner.managed.Managed` derived classes that represent output
        objects.  The return value of ``func`` will be stored and used by the pipelining
        system according to the specific details of the :py:class:`pypeliner.managed.Managed`
        derived class.

        """
        if not name:
            raise ValueError("job name not specified")
        job_ctx = self.ctx.copy()
        if ctx is not None:
            job_ctx.update(ctx)
        if name in self.job_definitions:
            raise ValueError('Job already defined')

        if sandbox is None:
            sandbox = self.default_sandbox
        self.job_definitions[name] = pypeliner.jobs.JobDefinition(
            name, axes, job_ctx, func, pypeliner.jobs.CallSet(ret=ret, args=args, kwargs=kwargs), sandbox=sandbox
        )

    def subworkflow(self, name='', axes=(), ctx=None, func=None, args=None, kwargs=None):
        """ Add a sub workflow to the pipeline.  A sub workflow is a set of jobs that
        takes the input dependencies and creates/updates output dependents.  The python
        function ``func`` should return a workflow object containing the set of jobs.

        :param name: unique name of the job, used to identify the job in logs and when
                     submitting instances to the exec queue
        :param axes: axes of the job.  defines the axes on which the job will operate.  A
                     job with an empty list for the axes has a single instance.  A job with
                     one axis in the axes list will have as many instances as were defined
                     for that axis by the split that is responsible for that axis.
        :param ctx: context of the subworkflow job as a dictionary of key, value pairs.  The context
                    is given to the exec queue and provides a way of communicating jobs
                    specific requirements such as memory and cpu usage.  Setting
                    ``ctx['local'] = True`` will result in the job being run locally on
                    the calling machine even when a cluster is being used.
        :param func: The function to call for this job.
        :param args: The list of positional arguments to be used for the function call.
        :param kwargs: The list of keyword arguments to be used for the function call.

        Any value in args or kwargs that is an instance of
        :py:class:`pypeliner.managed.Managed` will be resolved to a pipeline managed
        file or object at runtime.  See :py:mod:`pypeliner.managed`.

        """
        if not name:
            raise ValueError("subworkflow name not specified")
        job_ctx = self.ctx.copy()
        if ctx is not None:
            job_ctx.update(ctx)
        if name in self.job_definitions:
            raise ValueError('Job already defined')
        ret = pypeliner.managed.OutputWorkflow(name + '_workflow_definition', *axes)
        self.job_definitions[name] = pypeliner.jobs.SubWorkflowDefinition(
            name, axes, job_ctx, func, pypeliner.jobs.CallSet(ret=ret, args=args, kwargs=kwargs)
        )

    def _create_job_instances(self, graph, db):
        """ Create job instances from job definitions given resource and node managers,
        and a log directory.
        """
        for job_def in self.job_definitions.values():
            if hasattr(job_def, 'sandbox') and job_def.sandbox is not None:
                job_def.sandbox.create_conda_env(db.envs_dir)

        for job_def in self.job_definitions.values():
            for job_inst in job_def.create_job_instances(graph, db):
                yield job_inst
