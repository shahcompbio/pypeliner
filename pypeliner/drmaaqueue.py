import os
import drmaa

import delegator

class SubmitException(Exception):
    def __init__(self, message, command=None, filenames={}):
        self.message = message
        self.command = command
        self.filenames = filenames
    def __str__(self):
        except_str = 'Submit Failed!\n'
        except_str += self.message + '\n'
        if self.command is not None:
            if isinstance(self.command, str):
                except_str += 'command: ' + self.command + '\n'
            else:
                except_str += 'command: ' + ' '.join(self.command) + '\n'
        for name, filename in self.filenames.iteritems():
            try:
                header_written = False
                for line in open(filename, 'r'):
                    if not header_written:
                        except_str += '>>> ' + name + ' >>>\n'
                        header_written = True
                    except_str += line
                if header_written:
                    except_str += '<<< ' + name + ' <<<\n'
            except IOError:
                except_str += 'missing debug file: ' + name + ', ' + filename + '\n'
        return except_str

class DrmaaJob(object):
    """ Encapsulate a running job created using drmaa
    """
    def __init__(self, ctx, name, command, temps_prefix, native_spec, session):
        self.command = command
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = temps_prefix + '.job.out'
        self.debug_filenames['job stderr'] = temps_prefix + '.job.err'
        job_template = session.createJobTemplate()
        job_template.remoteCommand = command[0]
        job_template.args = command[1:]
        job_template.nativeSpecification = self.create_native_spec(native_spec, ctx)
        job_template.outputPath = ':' + self.debug_filenames['job stdout']
        job_template.errorPath = ':' + self.debug_filenames['job stderr']
        job_template.jobName = name
        self.job_id = session.runJob(job_template)
        session.deleteJobTemplate(job_template)
    def create_native_spec(self, native_spec, ctx):
        return '-w w ' + native_spec.format(**ctx)
    def finish(self, returncode):
        if (returncode != 0):
            raise SubmitException('submit command with return code ' + str(returncode), self.command, self.debug_filenames)

class DrmaaJobQueue:
    """ Maintain a list of running jobs executed synchronously using
    drmaa, with the ability to wait for jobs and return completed jobs
    """
    def __init__(self, temps_prefix, modules, native_spec):
        self.temps_prefix = temps_prefix
        self.modules = modules
        self.native_spec = native_spec
        self.jobs = dict()
    def __enter__(self):
        self.session = drmaa.Session()
        self.session.initialize()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.session.exit()
    def create(self, ctx, name, command, temps_prefix, native_spec, session):
        return DrmaaJob(ctx, name, command, temps_prefix, native_spec, session)
    def add(self, ctx, job):
        delegated = delegator.delegator(job, self.temps_prefix + job.filenamebase + '.delegator', self.modules)
        submitted = self.create(ctx, job.filenamebase, delegated.initialize(), self.temps_prefix + job.filenamebase + '.job', self.native_spec, self.session)
        self.jobs[submitted.job_id] = (job.id, delegated, submitted)
    def wait(self):
        try:
            job_info = self.session.wait(drmaa.Session.JOB_IDS_SESSION_ANY)
        except drmaa.InvalidJobException as e:
            if e.message.startswith('code 18'):
                self.jobs.clear()
            raise e
        job_id, delegator, submitted = self.jobs[job_info.jobId]
        del self.jobs[job_info.jobId]
        submitted.finish(job_info.exitStatus)
        return job_id, delegator.finalize()
    @property
    def length(self):
        return len(self.jobs)
    @property
    def empty(self):
        return len(self.jobs) == 0
