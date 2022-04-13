import functools
import hashlib
import logging
import os
import shutil

import pypeliner.commandline as cli
import yaml


class CondaSandbox(object):

    def __init__(self, channels=None, packages=None, pip_packages=None):
        if channels is None:
            channels = []
        if packages is None:
            packages = []
        if pip_packages is None:
            pip_packages = []
        self.channels = list(channels)
        self.packages = list(packages)
        self.pip_packages = list(pip_packages)
        self.prefix = None
        self.logger = logging.getLogger('pypeliner.sandbox')

    @property
    def config(self):
        return {'channels': self.channels, 'packages': self.packages, 'pip_packages': self.pip_packages}

    def _get_prefix(self):
        m = hashlib.md5()
        m.update(yaml.dump(self.config).encode())
        return m.hexdigest()

    def create_conda_env(self, env_dir):
        self.prefix = os.path.join(env_dir, self._get_prefix())
        env_config_file = os.path.join(self.prefix, 'sandbox_config.yaml')
        # TODO: Probably want some error checking on packages etc.
        if os.path.exists(self.prefix):
            with open(env_config_file, 'r') as fh:
                env_config = yaml.safe_load(fh)
            if (env_config['channels'] == self.channels) and (set(env_config['packages']) == set(self.packages)):
                return False
            else:
                shutil.rmtree(self.prefix)
        self._write_log()
        self._run_conda()
        with open(env_config_file, 'w') as out_fh:
            yaml.dump(self.config, out_fh)
        return True

    def wrap_function(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if self.prefix is None:
                raise Exception('Sandbox not initialized.')
            self._set_env()
            return func(*args, **kwargs)

        return wrapper

    def _run_conda(self):
        # TODO: Should add some retry logic in case of network timeouts
        cmd = ['conda', 'create', '--no-channel-priority', '--yes', '--quiet', '--prefix', self.prefix]
        for channel in self.channels:
            cmd.extend(['-c', channel])
        cmd.extend(self.packages + ['pip', ])
        cmd.extend(['>', '/dev/null'])
        cli.execute(*cmd)
        self._set_env()
        pip_exe = os.path.join(self.prefix, 'bin', 'pip')
        for p in self.pip_packages:
            cli.execute(pip_exe, 'install', p, '>', '/dev/null')

    def _set_env(self):
        os.environ['PATH'] = os.pathsep.join([os.path.join(self.prefix, 'bin'), os.environ['PATH']])
        os.environ['CONDA_PREFIX'] = self.prefix

    def _write_log(self):
        self.logger.info('sandbox {0} creating in {1}'.format(
            os.path.basename(self.prefix), self.prefix))
        self.logger.info('sandbox {0} conda channels {1}'.format(
            os.path.basename(self.prefix), ','.join(sorted(self.channels))))
        self.logger.info('sandbox {0} conda packages {1}'.format(
            os.path.basename(self.prefix), ','.join(sorted(self.packages))))
        self.logger.info('sandbox {0} pip packages {1}'.format(
            os.path.basename(self.prefix), ','.join(sorted(self.pip_packages))))
