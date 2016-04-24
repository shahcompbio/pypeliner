import pypeliner.commandline
import pypeliner.jobs


def _setobj_helper(value):
    return value


class Workflow(object):
    """ Contaner for a set of jobs making up a single workflow.

    """
    def __init__(self, default_ctx=None):
        self.default_ctx = {
            'mem': 4,
            'num_retry': 3,
            'mem_retry_factor': 2,
        }
        if default_ctx is not None:
            self.default_ctx.update(default_ctx)
        self.job_definitions = dict()

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
        name = '_'.join(('setobj', str(obj.name)) + obj.axes)
        self.transform(name=name, axes=axes, ctx={'immediate':True}, func=_setobj_helper, ret=obj, args=(value,))

    def commandline(self, name='', axes=(), ctx=None, args=None):
        """ Add a command line based transform to the pipeline

        This call is equivalent to::

            self.transform(name, axes, ctx, commandline.execute, None, *args)

        See :py:func:`pypeliner.scheduler.transform`

        """
        self.transform(name=name, axes=axes, ctx=ctx, func=pypeliner.commandline.execute, args=args)

    def transform(self, name='', axes=(), ctx=None, func=None, ret=None, args=None, kwargs=None):
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
        job_ctx = self.default_ctx.copy()
        if ctx is not None:
            job_ctx.update(ctx)
        if name in self.job_definitions:
            raise ValueError('Job already defined')
        self.job_definitions[name] = pypeliner.jobs.JobDefinition(name, axes, job_ctx, func, pypeliner.jobs.CallSet(ret=ret, args=args, kwargs=kwargs))

    def subworkflow(self, name='', axes=(), func=None, args=None, kwargs=None):
        """ Add a sub workflow to the pipeline.  A sub workflow is a set of jobs that
        takes the input dependencies and creates/updates output dependents.  The python 
        function ``func`` should return a workflow object containing the set of jobs.

        :param name: unique name of the job, used to identify the job in logs and when
                     submitting instances to the exec queue
        :param axes: axes of the job.  defines the axes on which the job will operate.  A
                     job with an empty list for the axes has a single instance.  A job with
                     one axis in the axes list will have as many instances as were defined
                     for that axis by the split that is responsible for that axis.
        :param func: The function to call for this job.
        :param args: The list of positional arguments to be used for the function call.
        :param kwargs: The list of keyword arguments to be used for the function call.

        Any value in args or kwargs that is an instance of
        :py:class:`pypeliner.managed.Managed` will be resolved to a pipeline managed
        file or object at runtime.  See :py:mod:`pypeliner.managed`.

        """
        if name in self.job_definitions:
            raise ValueError('Job already defined')
        self.job_definitions[name] = pypeliner.jobs.SubWorkflowDefinition(name, axes, func, pypeliner.jobs.CallSet(args=args, kwargs=kwargs))

    def _create_job_instances(self, graph, db):
        """ Create job instances from job definitions given resource and node managers,
        and a log directory.
        """
        for job_def in self.job_definitions.itervalues():
            for job_inst in job_def.create_job_instances(graph, db):
                yield job_inst

