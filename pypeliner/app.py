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
    argparser.add_argument('arg1', help='Additional argument 1')
    argparser.add_argument('arg2', help='Additional argument 2')
    args = vars(argparser.parse_args())

Create a :py:class:`pypeliner.app.Pypeline` object passing the arguments to the config parameter::

    pyp = pypeliner.app.Pypeline(config=args)

Create a workflow and run::

    workflow = pypeliner.workflow.Workflow()
    workflow.transform(...)
    workflow.commandline(...)

    pyp.run(workflow)

The following options are supported via the config dictionary argument to
:py:class:`pypeliner.app.Pypeline` or as command line arguments
by calling :py:func:`pypeliner.app.add_arguments` on an
``argparse.ArgumentParser`` object and passing the argument dictionary to
:py:class:`pypeliner.app.Pypeline`.

    tmpdir
        Location of pypeliner temporary files.

    pipelinedir
        Location of pypeliner database, logs.

    loglevel
        Logging level for console messages.  Valid logging levels are:
            - CRITICAL
            - ERROR
            - WARNING
            - INFO
            - DEBUG

    submit
        Type of exec queue to use for job submission.  If available, a default value
        will be obtained from the `$DEFAULT_SUBMITQUEUE` environment variable.

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

    interactive
        Run the pipeline in interactive mode, prompting at each step as to whether the job
        should be rerun.

    sentinel_only
        Run the pipeline in sentinal only mode, no time stamp checking on files for out
        of date status of jobs, rerun jobs based on whether they have already been
        run.

    context_config
        Run jobs within a specific type of container, either docker or singularity.
        config contains credentials for docker or path to dir with singularity containers

"""

import argparse
import datetime
import logging
import os
from collections import *

import pypeliner.execqueue
import pypeliner.execqueue.factory
import pypeliner.helpers
import pypeliner.runskip
import pypeliner.scheduler
import pypeliner.storage
import yaml

ConfigInfo = namedtuple('ConfigInfo', ['name', 'type', 'default', 'help'])

log_levels = ('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG')

default_submit_queue = os.environ.get('DEFAULT_SUBMITQUEUE', None)
default_submit_config = os.environ.get('DEFAULT_SUBMITCONFIG', None)
default_nativespec = os.environ.get('DEFAULT_NATIVESPEC', '')
default_storage_type = os.environ.get('DEFAULT_STORAGETYPE', 'local')
default_storage_config = os.environ.get('DEFAULT_STORAGECONFIG', None)

config_infos = list()
config_infos.append(ConfigInfo('tmpdir', str, './tmp', 'location of temporary files'))
config_infos.append(ConfigInfo('pipelinedir', str, None, 'location of pipeline files'))
config_infos.append(ConfigInfo('loglevel', log_levels, log_levels[2], 'logging level for console messages'))
config_infos.append(ConfigInfo('submit', str, default_submit_queue, 'job submission system'))
config_infos.append(ConfigInfo('submit_config', str, default_submit_config, 'job submission system config file'))
config_infos.append(ConfigInfo('nativespec', str, default_nativespec, 'qsub native specification'))
config_infos.append(ConfigInfo('storage', str, default_storage_type, 'file storage system'))
config_infos.append(ConfigInfo('storage_config', str, default_storage_config, 'file storage system config file'))
config_infos.append(ConfigInfo('maxjobs', int, 1, 'maximum number of parallel jobs'))
config_infos.append(ConfigInfo('repopulate', bool, False, 'recreate all temporaries'))
config_infos.append(ConfigInfo('rerun', bool, False, 'rerun the pipeline'))
config_infos.append(ConfigInfo('nocleanup', bool, False, 'do not automatically clean up temporaries'))
config_infos.append(ConfigInfo('interactive', bool, False, 'run in interactive mode'))
config_infos.append(ConfigInfo('sentinel_only', bool, False, 'no timestamp checks, sentinal only'))
config_infos.append(ConfigInfo('context_config', str, None, 'container registry credentials and job context overrides'))

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


def load_config(configfile):
    if not configfile:
        return
    with open(configfile) as configyaml:
        return yaml.safe_load(configyaml)


class Pypeline(object):
    """ Pipeline application helper class

    :param modules: list of modules pypeliner must import before running functions
                    for this pipeline.  These modules will be imported prior to
                    calling any of the user functions added to the pipeline.
    :param config: dictionary of configuration options.  See
                   :py:mod:`pypeliner.app` for available options.

    The Pipeline class sets up logging, creates an execution queue, and creates
    the scheduler with options provided by the config argument.

    """

    def __init__(self, modules=(), config=None):
        self.modules = modules
        self.config = dict([(info.name, info.default) for info in config_infos])
        if config is not None:
            self.config.update(config)

        # If no pipelinedir is specified, fall back to previous
        # functionality of having pipeline files in tmpdir, and
        # temporary files in a tmp subdirectory of tmpdir
        if self.config['pipelinedir'] is None:
            self.config['pipelinedir'] = self.config['tmpdir']
            self.config['tmpdir'] = os.path.join(self.config['tmpdir'], 'tmp')

        datetime_log_prefix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        self.logs_dir = os.path.join(self.config['pipelinedir'], 'log', datetime_log_prefix)
        self.pipeline_log_filename = os.path.join(self.logs_dir, 'pipeline.log')
        pypeliner.helpers.makedirs(self.logs_dir)
        pypeliner.helpers.symlink(self.logs_dir, os.path.join(self.config['pipelinedir'], 'log', 'latest'))

        logging.basicConfig(level=logging.DEBUG, filename=self.pipeline_log_filename, filemode='a')
        console = logging.StreamHandler()
        console.setLevel(self.config['loglevel'])
        logging.getLogger('').addHandler(console)
        logfmt = pypeliner.helpers.MultiLineFormatter('%(asctime)s - %(name)s - %(levelname)s - ')
        for handler in logging.root.handlers:
            handler.setFormatter(logfmt)

        # add json log file to the log_dir
        json_log_file = os.path.join(self.logs_dir, 'pipeline.json')
        jsonlog = logging.FileHandler(json_log_file, mode='a')
        jsonlog.setLevel(self.config['loglevel'])
        jsonlog.addFilter(logging.Filter('pypeliner'))
        logfmt = pypeliner.helpers.JsonFormatter()
        jsonlog.setFormatter(logfmt)
        logging.getLogger('').addHandler(jsonlog)

        self.exec_queue = pypeliner.execqueue.factory.create(
            self.config['submit'], modules=self.modules,
            native_spec=self.config['nativespec'],
            config_filename=self.config['submit_config'])

        self.file_storage = pypeliner.storage.create(
            self.config['storage'], workflow_dir=self.config['pipelinedir'])

        self.sch = pypeliner.scheduler.Scheduler()

        self.sch.temps_dir = self.config['tmpdir']
        self.sch.workflow_dir = self.config['pipelinedir']
        self.sch.logs_dir = self.logs_dir
        self.sch.max_jobs = int(self.config['maxjobs'])
        self.sch.cleanup = not self.config['nocleanup']
        pypeliner.helpers.GlobalState.set('context_config', load_config(self.config['context_config']))
        pypeliner.helpers.GlobalState.set('sentinel_only', self.config['sentinel_only'])

        if self.config['sentinel_only']:
            self.runskip = pypeliner.runskip.SentinalRunSkip(
                rerun=self.config['rerun'])
        else:
            self.runskip = pypeliner.runskip.BasicRunSkip(
                repopulate=self.config['repopulate'],
                rerun=self.config['rerun'])

        if self.config['interactive']:
            self.runskip = pypeliner.runskip.InteractiveRunSkip(self.runskip)

    def run(self, workflow):
        with self.exec_queue, self.file_storage:
            try:
                self.sch.run(workflow, self.exec_queue, self.file_storage, self.runskip)
            finally:
                self.runskip.close()
                print ('log file: ' + self.pipeline_log_filename)
