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
config_infos.append(ConfigInfo('config', str, None, 'configuration filename'))
config_infos.append(ConfigInfo('tmpdir', str, './tmp', 'location of intermediate pipeline files'))
config_infos.append(ConfigInfo('submit', ('local', 'qsub', 'asyncqsub', 'pbs', 'drmaa'), 'local', 'job submission system'))
config_infos.append(ConfigInfo('nativespec', str, '', 'qsub native specification'))
config_infos.append(ConfigInfo('maxjobs', int, 1, 'maximum number of parallel jobs'))
config_infos.append(ConfigInfo('repopulate', bool, False, 'recreate all temporaries'))
config_infos.append(ConfigInfo('rerun', bool, False, 'rerun the pipeline'))
config_infos.append(ConfigInfo('nocleanup', bool, False, 'do not automatically clean up temporaries'))
config_infos.append(ConfigInfo('noprune', bool, False, 'do not prune unecessary jobs'))
config_infos.append(ConfigInfo('pretend', bool, False, 'do nothing, report initial jobs to be run'))

config_defaults = dict()
for info in config_infos:
    if info.default is not None:
        config_defaults[info.name] = info.default
config_defaults['loglevel'] = logging.WARNING

class Config(object):
    def __init__(self, vars, section='main'):
        self._vars = vars
        self._section = section
        self._config = ConfigParser.ConfigParser()
        self._config.add_section(section)
        if self._vars.get('config', None) is not None:
            with open(self._vars['config'], 'r') as config_file:
                self._config.readfp(config_file)
        self.setup_groups()
    def setup_groups(self):
        self._prefixes = dict()
        for key, value in self._config.items(self._section):
            if key.endswith('_prefix'):
                self._prefixes[key[:-len('_prefix')]] = value
        self._suffixes = dict()
        for key, value in self._config.items(self._section):
            if key.endswith('_suffix'):
                self._suffixes[key[:-len('_suffix')]] = value
        self._groups = set(self._prefixes.keys() + self._suffixes.keys())
    def __getattr__(self, name):
        for group in self._groups:
            if name.endswith('_' + group):
                return self._prefixes.get(group, '') + name[:-len('_' + group)] + self._suffixes.get(group, '')
        if name in self._vars and self._vars[name] is not None:
            return self._vars[name]
        elif self._config.has_option(self._section, name):
            return self._config.get(self._section, name)
        elif name in config_defaults:
            return config_defaults[name]
        else:
            raise Exception('config option ' + name + ' not found')
    def __getstate__(self):
        return dict((member, self.__dict__[member]) for member in ('_vars', '_section', '_config'))
    def __setstate__(self, state):
        for member in ('_vars', '_section', '_config'):
            self.__dict__[member] = state[member]
        self.setup_groups()
    def __repr__(self):
        return __name__ + '.' + Config.__name__ + '(' + repr(self._vars) + ', ' + repr(self._section) + ')'

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
    def __init__(self, modules, config):
        self.modules = modules
        self.config = config
        datetime_log_prefix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        self.logs_dir = os.path.join(self.config.tmpdir, 'log', datetime_log_prefix)
        self.pipeline_log_filename = os.path.join(self.logs_dir, 'pipeline.log')
        helpers.makedirs(self.logs_dir)
        helpers.symlink(self.logs_dir, os.path.join(self.config.tmpdir, 'log', 'latest'))
        logging.basicConfig(level=logging.DEBUG, filename=self.pipeline_log_filename, filemode='a')
        console = logging.StreamHandler()
        if self.config.pretend:
            console.setLevel(logging.DEBUG)
        else:
            console.setLevel(self.config.loglevel)
        logging.getLogger('').addHandler(console)
        logfmt = helpers.MultiLineFormatter('%(asctime)s - %(name)s - %(levelname)s - ')
        for handler in logging.root.handlers:
            handler.setFormatter(logfmt)
        self.exec_queue = None
        if self.config.submit == 'local':
            self.exec_queue = pypeliner.execqueue.LocalJobQueue(self.modules)
        elif self.config.submit == 'qsub':
            self.exec_queue = pypeliner.execqueue.QsubJobQueue(self.modules, self.config.nativespec)
        elif self.config.submit == 'asyncqsub':
            self.exec_queue = pypeliner.execqueue.AsyncQsubJobQueue(self.modules, self.config.nativespec, 20)
        elif self.config.submit == 'pbs':
            self.exec_queue = pypeliner.execqueue.PbsJobQueue(self.modules, self.config.nativespec, 20)
        elif self.config.submit == 'drmaa':
            from pypeliner import drmaaqueue
            self.exec_queue = drmaaqueue.DrmaaJobQueue(self.modules, self.config.nativespec)
        self.sch = pypeliner.scheduler.Scheduler()
        self.sch.set_pipeline_dir(self.config.tmpdir)
        self.sch.logs_dir = self.logs_dir
        self.sch.max_jobs = int(self.config.maxjobs)
        self.sch.repopulate = self.config.repopulate
        self.sch.rerun = self.config.rerun
        self.sch.cleanup = not self.config.nocleanup
        self.sch.prune = not self.config.noprune
    def run(self):
        if self.config.pretend:
            return self.sch.pretend()
        else:
            with self.exec_queue:
                return self.sch.run(self.exec_queue)
