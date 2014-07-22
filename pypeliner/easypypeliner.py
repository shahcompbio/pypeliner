import sys
import logging
import os
import ConfigParser
import argparse
import string
import datetime
from collections import *

import pypeliner
import helpers

ConfigInfo = namedtuple('ConfigInfo', ['name', 'type', 'default', 'help'])

config_infos = list()
config_infos.append(ConfigInfo('tmpdir', str, './tmp', 'location of intermediate pipeline files'))
config_infos.append(ConfigInfo('submit', ('local', 'qsub', 'asyncqsub', 'pbs', 'drmaa'), 'local', 'job submission system'))
config_infos.append(ConfigInfo('nativespec', str, '', 'qsub native specification'))
config_infos.append(ConfigInfo('maxjobs', int, 1, 'maximum number of parallel jobs'))
config_infos.append(ConfigInfo('repopulate', bool, False, 'recreate all temporaries'))
config_infos.append(ConfigInfo('rerun', bool, False, 'rerun the pipeline'))
config_infos.append(ConfigInfo('nocleanup', bool, False, 'do not automatically clean up temporaries'))
config_infos.append(ConfigInfo('noprune', bool, False, 'do not prune unecessary jobs'))
config_infos.append(ConfigInfo('pretend', bool, False, 'do nothing, report initial jobs to be run'))

config_defaults = dict([(info.name, info.default) for info in config_infos])
config_defaults['loglevel'] = logging.WARNING

def add_argument(argparser, config_info):
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
    group = argparser.add_argument_group('pypeliner arguments')
    for config_info in config_infos:
        add_argument(group, config_info)
    group.add_argument('--debug', help='Print debug info', action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.WARNING)
    group.add_argument('--verbose', help='Verbose', action="store_const", dest="loglevel", const=logging.INFO)

class EasyPypeliner(object):
    ''' Helper class for easy logging, scheduler setup '''
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
        self.sch.set_pipeline_dir(self.config['tmpdir'])
        self.sch.logs_dir = self.logs_dir
        self.sch.max_jobs = int(self.config['maxjobs'])
        self.sch.repopulate = self.config['repopulate']
        self.sch.rerun = self.config['rerun']
        self.sch.cleanup = not self.config['nocleanup']
        self.sch.prune = not self.config['noprune']

    def run(self):
        if self.config['pretend']:
            return self.sch.pretend()
        else:
            with self.exec_queue:
                return self.sch.run(self.exec_queue)
