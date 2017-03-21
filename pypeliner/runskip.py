import cmd
import fnmatch


class BasicRunSkip(object):
    def __init__(self, repopulate=False, rerun=False):
        self.repopulate = repopulate
        self.rerun = rerun

    def __call__(self, job):
        if self.rerun:
            return True
        if job.out_of_date():
            return True
        if job.is_required_downstream:
            return True
        if self.repopulate and job.output_missing():
            return True
        return False

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

    def print_patterns(self):
        for key, match in self._patterns:
            print key, match


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
        print 'run the job'

    def do_skip(self, line):
        return self.do_command('skip', line)
    
    def help_skip(self):
        print 'skip the job'

    def do_verify(self, line):
        return self.do_command('verify', line)

    def help_verify(self):
        print 'skip the job it doesnt need to be run'

    def do_touch(self, line):
        return self.do_command('touch', line)

    def help_touch(self):
        print 'touch outputs and skip the job'

    def do_default(self, line):
        return self.do_command('default', line)

    def help_default(self):
        print 'default behaviour'


class InteractiveRunSkip(object):
    def __init__(self, default):
        self.patterns = PatternMatcher()
        self.default = default

    def __call__(self, job):
        runskip_cmd = RunSkipCmd(self.patterns)

        while True:
            if runskip_cmd.command == 'run' or self.patterns.get(job.displayname) == 'run':
                return True

            if runskip_cmd.command == 'skip' or self.patterns.get(job.displayname) == 'skip':
                return False

            if runskip_cmd.command == 'verify' or self.patterns.get(job.displayname) == 'verify':
                if not self.default(job):
                    return False

            if runskip_cmd.command == 'touch' or self.patterns.get(job.displayname) == 'touch':
                job.touch_outputs()
                return False

            if runskip_cmd.command == 'default' or self.patterns.get(job.displayname) == 'default':
                return self.default(job)

            runskip_cmd.cmdloop()
            if not runskip_cmd.success:
                raise Exception('failed to obtain user input')

    def close(self):
        print 'run skip commands:'
        self.patterns.print_patterns()


