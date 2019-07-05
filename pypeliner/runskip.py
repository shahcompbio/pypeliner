import cmd
import fnmatch
import logging


class BasicRunSkip(object):
    def __init__(self, repopulate=False, rerun=False):
        self.repopulate = repopulate
        self.rerun = rerun

    def __call__(self, job):
        if job.runskip_request == "skip":
            return False, 'skip requested'
        if self.rerun:
            return True, 'rerun requested\n' + job.explain_out_of_date()
        if job.out_of_date():
            return True, job.explain_out_of_date()
        if job.is_required_downstream:
            return True, 'required downstream\n' + job.explain_out_of_date()
        if self.repopulate and job.output_missing():
            return True, 'repopulate requested, missing output\n' + job.explain_out_of_date()
        return False, job.explain_out_of_date()

    def close(self):
        pass


class SentinalRunSkip(object):
    def __init__(self, rerun=False):
        self.rerun = rerun

    def __call__(self, job):
        if job.runskip_request == "skip":
            return False, 'skip requested'
        if self.rerun:
            return True, 'rerun requested\n' + job.explain_out_of_date()
        if job.already_run():
            return False, 'already run'
        return True, 'not yet run'

    def close(self):
        pass


class PatternMatcher(object):
    def __init__(self):
        self._patterns = list()

    def get(self, value):
        for key, match in self._patterns:
            if fnmatch.fnmatch(value, match):
                return key

    def add(self, key, match):
        self._patterns.append((key, match))

    def get_patterns_str(self):
        patterns = []
        for key, match in self._patterns:
            patterns.append('{} {}'.format(key, match))
        return '\n'.join(patterns)


class RunSkipCmd(cmd.Cmd):
    def __init__(self, patterns):
        cmd.Cmd.__init__(self)
        self.patterns = patterns
        self.command = ''
        self.success = False
        self.prompt = 'pypeliner$ '

    def do_command(self, command, line):
        if len(line) > 0:
            self.patterns.add(command, line)
        else:
            self.command = command
        self.success = True
        return True

    def do_run(self, line):
        return self.do_command('run', line)

    def help_run(self):
        print ('run the job')

    def do_skip(self, line):
        return self.do_command('skip', line)

    def help_skip(self):
        print ('skip the job')

    def do_verify(self, line):
        return self.do_command('verify', line)

    def help_verify(self):
        print ('skip the job it doesnt need to be run')

    def do_touch(self, line):
        return self.do_command('touch', line)

    def help_touch(self):
        print ('touch outputs and skip the job')

    def do_default(self, line):
        return self.do_command('default', line)

    def help_default(self):
        print ('default behaviour')


class InteractiveRunSkip(object):
    def __init__(self, default):
        self.patterns = PatternMatcher()
        self.default = default
        self._logger = logging.getLogger('pypeliner.scheduler.runskip')

    def __call__(self, job):
        default_is_run_required, default_explaination = self.default(job)

        self._logger.info(
            'job ' + job.displayname + ' default run: ' + str(
                default_is_run_required) + ' default explanation: ' + default_explaination,
            extra={
                'job_name': job.displayname, "explanation": default_explaination,
                'task_name': job.id[1]
            }
        )

        runskip_cmd = RunSkipCmd(self.patterns)

        while True:
            if runskip_cmd.command == 'run' or self.patterns.get(job.displayname) == 'run':
                return True, 'run requested'

            if runskip_cmd.command == 'skip' or self.patterns.get(job.displayname) == 'skip':
                return False, 'skip requested'

            if runskip_cmd.command == 'verify' or self.patterns.get(job.displayname) == 'verify':
                if not default_is_run_required:
                    return False, 'verified and skipped'

            if runskip_cmd.command == 'touch' or self.patterns.get(job.displayname) == 'touch':
                job.touch_outputs()
                return False, 'touch requested'

            if runskip_cmd.command == 'default' or self.patterns.get(job.displayname) == 'default':
                return default_is_run_required, 'default requested'

            runskip_cmd.cmdloop()
            if not runskip_cmd.success:
                raise Exception('failed to obtain user input')

    def close(self):
        self._logger.info('run skip commands:\n' + self.patterns.get_patterns_str())

