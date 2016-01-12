"""
Pipeline application functionality

Provides classes and functions to help creation of a pipeline application.
Using this module, it should be possible to create a pipeline application
with the full functionality of pypeliner with only a few lines of code.
Typically, the first few lines of a pipeline application would be as follows.

Import pypeliner::

    import pypeliner

Create an ``argparse.ArgumentParser`` object to handle command line arguments
including a config, then parse the arguments::

    argparser = argparse.ArgumentParser()
    pypeliner.app.add_arguments(argparser)
    argparser.add_argument('config', help='Configuration Filename')
    argparser.add_argument('arg1', help='Additional argument 1')
    argparser.add_argument('arg2', help='Additional argument 2')
    args = vars(argparser.parse_args())

Read in the config.  Here we are using a python syntax style config::

    config = {}
    execfile(args['config'], config)

Override config options with command line arguments::

    config.update(args)

Create a :py:class:`pypeliner.app.Pypeline` object with the config::

    pyp = pypeliner.app.Pypeline([], config)

Add jobs and run::

    pyp.sch.transform(...)
    pyp.sch.commandline(...)
    pyp.sch.run()

The following options are supported via the config dictionary argument to 
:py:class:`pypeliner.app.Pypeline` or as command line arguments
by calling :py:func:`pypeliner.app.add_arguments` on an
``argparse.ArgumentParser`` object and passing the argument dictionary to
:py:class:`pypeliner.app.Pypeline`.

    tmpdir
        Location of pypeliner database, temporary files and logs.

    loglevel
        Logging level for console messages.  Valid logging levels are:
            - CRITICAL
            - ERROR
            - WARNING
            - INFO
            - DEBUG

    submit
        Type of exec queue to use for job submission.

    nativespec
        Required for qsub based job submission, specifies how to request memory from
        the cluster system.  Nativespec is treated as a python style format string
        and uses the job's context to resolve named.  If available, a default value
        will be obtained from the `$DEFAULT_NATIVESPEC` environment variable.

    maxjobs
        The maximum number of jobs to execute in parallel, either on a cluster or
        using subprocess to create multiple processes.

    repopulate
        Recreate all temporary files that may have been cleaned up during a previous
        run in which garbage collection was enabled.  Files may be subsequently 
        garbage collected after creation depending on the `nocleanup` option.

    nocleanup
        Do not clean up temporary files.  Does not recreate files that were garbage
        collected in a previous run of the pipeline.

    noprune
        Do not prune jobs that are not necessary for the requested outputs.  Run
        all jobs.

    pretend
        Do not run any jobs, instead just print what would be run.

"""

import sys
import logging
import os
import argparse
import string
import datetime
from collections import *

import pypeliner
import helpers

ConfigInfo = namedtuple('ConfigInfo', ['name', 'type', 'default', 'help'])

log_levels = ('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG')
submit_types = ('local', 'qsub', 'asyncqsub', 'pbs', 'drmaa')

default_nativespec = os.environ.get('DEFAULT_NATIVESPEC', '')

config_infos = list()
config_infos.append(ConfigInfo('tmpdir', str, './tmp', 'location of intermediate pipeline files'))
config_infos.append(ConfigInfo('loglevel', log_levels, log_levels[2], 'logging level for console messages'))
config_infos.append(ConfigInfo('submit', submit_types, submit_types[0], 'job submission system'))
config_infos.append(ConfigInfo('nativespec', str, default_nativespec, 'qsub native specification'))
config_infos.append(ConfigInfo('maxjobs', int, 1, 'maximum number of parallel jobs'))
config_infos.append(ConfigInfo('repopulate', bool, False, 'recreate all temporaries'))
config_infos.append(ConfigInfo('rerun', bool, False, 'rerun the pipeline'))
config_infos.append(ConfigInfo('nocleanup', bool, False, 'do not automatically clean up temporaries'))
config_infos.append(ConfigInfo('noprune', bool, False, 'do not prune unecessary jobs'))
config_infos.append(ConfigInfo('pretend', bool, False, 'do nothing, report initial jobs to be run'))

config_defaults = dict([(info.name, info.default) for info in config_infos])


def _add_argument(argparser, config_info):
    args = list()
    kwargs = dict()
    args.append('--' + config_info.name)
    kwargs['default'] = argparse.SUPPRESS
    if type(config_info.type) == tuple:
        kwargs['choices'] = config_info.type
    elif config_info.type == bool:
        kwargs['action'] = 'store_true'
        kwargs['default'] = False
    else:
        kwargs['type'] = config_info.type
    kwargs['help'] = config_info.help
    argparser.add_argument(*args, **kwargs)

def add_arguments(argparser):
    """ Add pypeliner arguments to an argparser object

    :param argparser: ``argparse.ArgumentParser`` object to which pypeliner specific 
                      arguments should be added.  See
                      :py:mod:`pypeliner.app` for available options.

    Add arguments to an ``argparse.ArgumentParser`` object to allow command line control
    of pypeliner.  The options should be extracted after calling 
    ``argparse.ArgumentParser.parse_args``, converted to a dictionary using `vars`
    and provided to the initializer of a :py:mod:`pypeliner.app.Pypeline`
    object.

    """
    group = argparser.add_argument_group('pypeliner arguments')
    for config_info in config_infos:
        _add_argument(group, config_info)


class Pypeline(object):
    """ Pipeline application helper class

    :param modules: list of modules pypeliner must import before running functions
                    for this pipeline.  These modules will be imported prior to
                    calling any of the user functions added to the pipeline.
    :param config: dictionary of configuration options.  See
                   :py:mod:`pypeliner.app` for available options.

    The Pipeline class sets up logging, creates an execution queue, and creates
    the scheduler with options provided by the conifg argument.

    """
    def __init__(self, modules, config):
        self.modules = modules
        self.config = dict([(info.name, info.default) for info in config_infos])
        self.config.update(config)

        datetime_log_prefix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        self.logs_dir = os.path.join(self.config['tmpdir'], 'log', datetime_log_prefix)
        self.pipeline_log_filename = os.path.join(self.logs_dir, 'pipeline.log')
        helpers.makedirs(self.logs_dir)
        helpers.symlink(self.logs_dir, os.path.join(self.config['tmpdir'], 'log', 'latest'))
        logging.basicConfig(level=logging.DEBUG, filename=self.pipeline_log_filename, filemode='a')
        console = logging.StreamHandler()
        if self.config['pretend']:
            console.setLevel(logging.DEBUG)
        else:
            console.setLevel(self.config['loglevel'])
        logging.getLogger('').addHandler(console)
        logfmt = helpers.MultiLineFormatter('%(asctime)s - %(name)s - %(levelname)s - ')
        for handler in logging.root.handlers:
            handler.setFormatter(logfmt)

        self.exec_queue = None
        if self.config['submit'] == 'local':
            self.exec_queue = pypeliner.execqueue.LocalJobQueue(self.modules)
        elif self.config['submit'] == 'qsub':
            self.exec_queue = pypeliner.execqueue.QsubJobQueue(self.modules, self.config['nativespec'])
        elif self.config['submit'] == 'asyncqsub':
            self.exec_queue = pypeliner.execqueue.AsyncQsubJobQueue(self.modules, self.config['nativespec'], 20)
        elif self.config['submit'] == 'pbs':
            self.exec_queue = pypeliner.execqueue.PbsJobQueue(self.modules, self.config['nativespec'], 20)
        elif self.config['submit'] == 'drmaa':
            from pypeliner import drmaaqueue
            self.exec_queue = drmaaqueue.DrmaaJobQueue(self.modules, self.config['nativespec'])

        self.sch = pypeliner.scheduler.Scheduler()
        """ :py:class:`pypeliner.scheduler.Scheduler` object to which jobs are added using 
        :py:func:`pypeliner.scheduler.Scheduler.transform` etc. """

        self.sch.workflow_dir = self.config['tmpdir']
        self.sch.logs_dir = self.logs_dir
        self.sch.max_jobs = int(self.config['maxjobs'])
        self.sch.repopulate = self.config['repopulate']
        self.sch.rerun = self.config['rerun']
        self.sch.cleanup = not self.config['nocleanup']
        self.sch.prune = not self.config['noprune']

    def run(self, workflow):
        if self.config['pretend']:
            return self.sch.pretend()
        else:
            with self.exec_queue:
                return self.sch.run(workflow, self.exec_queue)
