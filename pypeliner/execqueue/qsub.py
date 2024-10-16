import json
import logging
import os
import subprocess
from string import Formatter

import pypeliner.delegator
import pypeliner.execqueue.local
import pypeliner.execqueue.qcmd
import pypeliner.execqueue.utils
import pypeliner.helpers
import time


class NativespecFormatter(dict):
    def __missing__(self, key):
        if '=' in key:
            assert len(key.split('=')) == 2
            key, default = key.split('=')
            return self.get(key, default)
        else:
            raise KeyError('Key {} missing in job context'.format(key))


class QsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess
    """

    def __init__(self, ctx, name, sent, temps_dir, modules, qsub_bin, native_spec):
        self.name = name
        self.logger = logging.getLogger('pypeliner.execqueue')
        self.delegated = pypeliner.delegator.Delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(temps_dir, 'submit.err')
        self.debug_files = []
        self.script_filename = os.path.join(temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write("#!/bin/sh\n")
            script_file.write(' '.join(self.command) + '\n')
        pypeliner.helpers.set_executable(self.script_filename)
        self.submit_command = self.create_submit_command(
            ctx, self.name, self.script_filename, qsub_bin, native_spec,
            self.debug_filenames['job stdout'],
            self.debug_filenames['job stderr']
        )
        try:
            self.debug_files.append(open(self.debug_filenames['submit stdout'], 'w'))
            self.debug_files.append(open(self.debug_filenames['submit stderr'], 'w'))
            self.process = subprocess.Popen(self.submit_command, stdout=self.debug_files[0], stderr=self.debug_files[1])
        except OSError as e:
            self.close_debug_files()
            error_text = self.name + ' submit failed\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            if type(e) == OSError and e.errno == 2:
                error_text += str(e) + ', ' + self.submit_command[0] + ' not found\n'
            else:
                error_text += str(e) + '\n'
            error_text += pypeliner.execqueue.utils.log_text(self.debug_filenames)
            self.logger.error(error_text,
                              extra={"id": self.name, "status": "submit_fail", "type": "job"})
            raise pypeliner.execqueue.base.SubmitError()

    def create_submit_command(
            self, ctx, name, script_filename, qsub_bin, native_spec,
            stdout_filename, stderr_filename
    ):
        qsub = [qsub_bin]
        qsub += ['-sync', 'y', '-b', 'y']
        qsub += Formatter().vformat(native_spec, (), NativespecFormatter(**ctx))
        qsub += ['-N', pypeliner.execqueue.utils.qsub_format_name(name)]
        qsub += ['-o', stdout_filename]
        qsub += ['-e', stderr_filename]
        qsub += [script_filename]
        return qsub

    def close_debug_files(self):
        for file in self.debug_files:
            file.close()

    def finalize(self, returncode):
        self.close_debug_files()
        self.received = self.delegated.finalize()
        if returncode != 0 or self.received is None or not self.received.started:
            error_text = self.name + ' failed to complete\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            error_text += 'return code {0}\n'.format(returncode)
            error_text += pypeliner.execqueue.utils.log_text(self.debug_filenames)
            self.logger.error(error_text,
                              extra={"id": self.name, "status": "fail", "type": "job"})
            raise pypeliner.execqueue.base.ReceiveError()


class QsubJobQueue(pypeliner.execqueue.subproc.SubProcessJobQueue):
    """ Queue of qsub jobs """

    def __init__(self, modules=None, native_spec=None):
        super(QsubJobQueue, self).__init__(modules)
        self.qsub_bin = pypeliner.helpers.which('qsub')
        self.native_spec = native_spec
        self.local_queue = pypeliner.execqueue.local.LocalJobQueue(modules)

    def create(self, ctx, name, sent, temps_dir):
        if ctx.get('local', False):
            return pypeliner.execqueue.local.LocalJob(ctx, name, sent, temps_dir, self.modules)
        else:
            return QsubJob(ctx, name, sent, temps_dir, self.modules, self.qsub_bin, self.native_spec)


class AsyncQsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess, and polled using qstat
    """

    def __init__(
            self, ctx, name, sent, temps_dir, modules, qenv, native_spec,
            qstat_job_status, qsub_wrapper, qacct_wrapper
    ):
        self.name = name
        self.qenv = qenv
        self.qstat_job_status = qstat_job_status
        self.qsub_job_id = None
        self.qacct = None
        self.logger = logging.getLogger('pypeliner.execqueue')

        self.delegated = pypeliner.delegator.Delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()

        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(temps_dir, 'submit.err')
        self.debug_filenames['qacct stdout'] = os.path.join(temps_dir, 'qacct.out')
        self.debug_filenames['qacct stderr'] = os.path.join(temps_dir, 'qacct.err')
        for filename in self.debug_filenames.values():
            pypeliner.helpers.saferemove(filename)

        self.script_filename = os.path.join(temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write("#!/bin/sh\n")
            script_file.write(' '.join(self.command) + '\n')
        pypeliner.helpers.set_executable(self.script_filename)

        self.qsub = qsub_wrapper(
            self.qenv, ctx, name, self.script_filename, native_spec, self.debug_filenames['job stdout'],
            self.debug_filenames['job stderr'], self.debug_filenames['submit stdout'],
            self.debug_filenames['submit stderr'])

        self.submit_command = self.qsub.submit_command
        self.qsub_job_id = self.qsub.submit_job()

        self.qsub_time = time.time()

        self.qacct = qacct_wrapper(
            self.qenv, self.qsub_job_id,
            self.debug_filenames['qacct stdout'],
            self.debug_filenames['qacct stderr'],
        )

    @property
    def finished(self):
        """ Get job finished boolean.
        """
        if not self.qstat_job_status.finished(self.qsub_job_id, self.qsub_time) and \
                not self.qstat_job_status.errors(self.qsub_job_id):
            return False

        if self.qstat_job_status.errors(self.qsub_job_id):
            try:
                self.qacct.check()
            except pypeliner.execqueue.qcmd.QacctError:
                pass

        return True

    def finalize(self):
        """ Finalize a job after remote run.
        """
        assert self.finished

        if self.qstat_job_status.errors(self.qsub_job_id):
            self.delete()
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('job error'))

        if self.qacct.results is not None and self.qacct.results['exit_status'] != '0':
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('qsub error'))

        self.received = self.delegated.finalize()

        if self.received is None or not self.received.started:
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('receive error'))

    def delete(self):
        """ Delete job from queue.
        """
        try:
            subprocess.check_call([self.qenv.qdel_bin, self.qsub_job_id])
        except:
            self.logger.error('Unable to delete {}'.format(self.qsub_job_id),
                              extra={"id": self.name, "status": "could_not_delete_job", "type": "job"})

    def create_error_text(self, desc):
        """ Create error text string.

        Args:
            desc (str): error description

        Returns:
            str: multi-line error text
        """

        error_text = list()

        error_text += [desc + ' for job: ' + self.name]

        if self.qsub_job_id is not None:
            error_text += ['qsub id: ' + str(self.qsub_job_id)]

        error_text += ['delegator command: ' + ' '.join(self.command)]
        error_text += ['submit command: ' + ' '.join(self.submit_command)]

        if self.qacct is not None and self.qacct.results is not None:
            error_text += ['memory consumed: ' + self.qacct.results['maxvmem']]
            error_text += ['job exit status: ' + self.qacct.results['exit_status']]

        for debug_type, debug_filename in self.debug_filenames.items():
            if not os.path.exists(debug_filename):
                error_text += [debug_type + ': missing']
                continue
            with open(debug_filename, 'r') as debug_file:
                error_text += [debug_type + ':']
                for line in debug_file:
                    error_text += ['\t' + line.rstrip()]

        return '\n'.join(error_text)


class AsyncQsubJobQueue(pypeliner.execqueue.base.JobQueue):
    """ Class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """

    def __init__(self, modules=None, **kwargs):
        self.modules = modules
        self.native_spec = kwargs['native_spec']
        self.qstat = pypeliner.execqueue.qcmd.QstatJobStatus(self.qenv)
        self.jobs = dict()
        self.name_islocal = dict()
        self.local_queue = pypeliner.execqueue.local.LocalJobQueue(modules)

    def __enter__(self):
        self.local_queue.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.local_queue.__exit__(exc_type, exc_value, traceback)
        for job in self.jobs.values():
            job.delete()

    def create(self, ctx, name, sent, temps_dir):
        return AsyncQsubJob(ctx, name, sent, temps_dir, self.modules, self.qenv, self.native_spec, self.qstat,
                            pypeliner.execqueue.qcmd.QsubWrapper, pypeliner.execqueue.qcmd.QacctWrapper)

    def send(self, ctx, name, sent, temps_dir):
        if ctx.get('local', False):
            self.local_queue.send(ctx, name, sent, temps_dir)
        else:
            self.jobs[name] = self.create(ctx, name, sent, temps_dir)

    def wait(self):
        while True:
            if not self.local_queue.empty:
                name = self.local_queue.wait(immediate=True)
                if name is not None:
                    self.name_islocal[name] = True
                    return name
            for name, job in self.jobs.items():
                if job.finished:
                    return name
            self.qstat.update(self.jobs)

    def receive(self, name):
        if self.name_islocal.pop(name, False):
            return self.local_queue.receive(name)
        job = self.jobs.pop(name)
        job.finalize()
        return job.received

    @property
    def length(self):
        return len(self.jobs) + self.local_queue.length

    @property
    def empty(self):
        return self.length == 0

    @property
    def qenv(self):
        return pypeliner.execqueue.qcmd.QEnv()


class PbsQstatJobStatus(pypeliner.execqueue.qcmd.QstatJobStatus):
    """ Statuses of jobs on a pbs cluster """

    def finished(self):
        return 'c' in self.cached_job_status.get(self.job_id, 'c')

    def errors(self, job_id):
        return False


class PbsJobQueue(AsyncQsubJobQueue):
    """ Queue of jobs running on a pbs cluster """

    def __init__(self, modules=None, **kwargs):
        super(PbsJobQueue, self).__init__(modules, **kwargs)
        self.qstat = PbsQstatJobStatus(self.qenv)


class LsfQstatJobStatus(pypeliner.execqueue.qcmd.QstatJobStatus):
    """ Statuses of jobs on a lfs cluster """

    def get_qstat_job_status(self, jobs):
        job_status = dict()

        qsub_ids = [val.qsub_job_id for val in jobs.values()]

        qsub_id_groups = [qsub_ids[x:x + 100] for x in range(0, len(qsub_ids), 100)]

        for ids_group in qsub_id_groups:
            cmd = [self.qenv.qstat_bin, '-o', 'JOBID:10 STAT:6', '-json']
            cmd.extend(ids_group)

            qstat_output = subprocess.check_output(cmd).decode()
            qstat_output = json.loads(qstat_output)

            for record in qstat_output['RECORDS']:
                if 'ERROR' in record:
                    continue
                if record['STAT'] not in ['PEND', 'WAIT', 'PROV', 'RUN']:
                    continue
                try:
                    job_status[record['JOBID']] = record['STAT'].lower()
                except IndexError:
                    continue
        return job_status

    def errors(self, qsub_job_id):
        if self.cached_job_status is None:
            return None
        # possible error states are POST_ERR state if it fails, or {P|U|S}SUSP states
        return 'ERR' in self.cached_job_status.get(qsub_job_id, '') or \
            'SUSP' in self.cached_job_status.get(qsub_job_id, '')


class SlurmQstatJobStatus(pypeliner.execqueue.qcmd.QstatJobStatus):
    """ Statuses of jobs on a lfs cluster """

    def get_qstat_job_status(self, jobs):
        job_status = dict()

        qsub_ids = [val.qsub_job_id for val in jobs.values()]

        qsub_id_groups = [qsub_ids[x:x + 100] for x in range(0, len(qsub_ids), 100)]

        for ids_group in qsub_id_groups:
            cmd = [self.qenv.qstat_bin, '-j', ','.join([str(v) for v in ids_group])]
            qstat_output = subprocess.check_output(cmd).decode().split('\n')
            assert len(qstat_output) > 1
            header = {v: i for i, v in enumerate(qstat_output[0].strip().split())}

            for line in qstat_output[1:]:
                line = line.strip().split()
                if len(line) == 0:
                    continue
                status = line[header['ST']]
                jobid = int(line[header['JOBID']])

                if status not in ['CF', 'CG', 'PD', 'R', 'RD', 'RF', 'RH', 'RQ', 'RS', 'RV', 'SI', 'SE', 'SO']:
                    continue

                job_status[jobid] = status

        return job_status

    def errors(self, qsub_job_id):
        if self.cached_job_status is None:
            return None
        # possible error states are POST_ERR state if it fails, or {P|U|S}SUSP states
        return 'F' in self.cached_job_status.get(qsub_job_id, '') or \
            'S' in self.cached_job_status.get(qsub_job_id, '') or \
            'OOM' in self.cached_job_status.get(qsub_job_id, '') or \
            'BF' in self.cached_job_status.get(qsub_job_id, '') or \
            'CA' in self.cached_job_status.get(qsub_job_id, '') or \
            'DL' in self.cached_job_status.get(qsub_job_id, '') or \
            'SE' in self.cached_job_status.get(qsub_job_id, '') or \
            'ST' in self.cached_job_status.get(qsub_job_id, '') or \
            'TO' in self.cached_job_status.get(qsub_job_id, '')


class LsfJobQueue(AsyncQsubJobQueue):
    """ Queue of jobs running on a lfs cluster """

    def __init__(self, modules=None, **kwargs):
        super(LsfJobQueue, self).__init__(modules, **kwargs)
        self.qstat = LsfQstatJobStatus(self.qenv)

    def create(self, ctx, name, sent, temps_dir):
        """create job"""
        return AsyncQsubJob(ctx, name, sent, temps_dir, self.modules, self.qenv, self.native_spec, self.qstat,
                            pypeliner.execqueue.qcmd.LsfsubWrapper, pypeliner.execqueue.qcmd.LsfacctWrapper)

    @property
    def qenv(self):
        return pypeliner.execqueue.qcmd.LsfEnv()


class SlurmJobQueue(AsyncQsubJobQueue):
    """ Queue of jobs running on a lfs cluster """

    def __init__(self, modules=None, **kwargs):
        super(SlurmJobQueue, self).__init__(modules, **kwargs)
        self.qstat = SlurmQstatJobStatus(self.qenv)

    def create(self, ctx, name, sent, temps_dir):
        """create job"""
        return AsyncQsubJob(ctx, name, sent, temps_dir, self.modules, self.qenv, self.native_spec, self.qstat,
                            pypeliner.execqueue.qcmd.SlurmsubWrapper, pypeliner.execqueue.qcmd.SlurmacctWrapper)

    @property
    def qenv(self):
        return pypeliner.execqueue.qcmd.SlurmEnv()
