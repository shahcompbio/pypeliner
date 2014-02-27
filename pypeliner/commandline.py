import subprocess
import sys

class CommandLineException(Exception):
    def __init__(self, args, command, returncode):
        self.args = args
        self.command = command
        self.returncode = returncode
    def __str__(self):
        return "Command '%s' with return code %d in command line `%s`" % (self.command, self.returncode, " ".join(self.args))

class CommandNotFoundException(Exception):
    def __init__(self, args, command):
        self.args = args
        self.command = command
    def __str__(self):
        return "Command '%s' not found in command line `%s`" % (self.command, " ".join(self.args))


def execute(*args):

    if args.count(">") > 1 or args[0] == ">" or args[-1] == ">":
        raise Exception("Bad redirect to file")

    if args.count("<") > 1 or args[0] == "<" or args[-1] == "<":
        raise Exception("Bad redirect from file")

    args = filter(lambda arg: arg != '', args)
    args = list(str(arg) for arg in args)
    command_list = list(_split_list(args, "|"))

    # Determine input filename for redirect if any
    input = None
    if command_list[0].count("<") == 1:
        input = _get_next(command_list[0],"<")
        command_list[0].remove("<")
        command_list[0].remove(input)

    # Determine output filename for redirect if any
    output = None
    if command_list[-1].count(">") == 1:
        output = _get_next(command_list[-1],">")
        command_list[-1].remove(">")
        command_list[-1].remove(output)

    # Call process dependent on input / output redirects
    if input and output:
        # Call processes with both input and output
        with open(input, 'rb') as infile, open(output, 'wb') as outfile:
            _call_processes(args, command_list, infile, outfile)
    elif input:
        # Call processes with input
        with open(input, 'rb') as infile:
            _call_processes(args, command_list, infile, sys.stdout)
    elif output:
        # Call processes with input
        with open(output, 'wb') as outfile:
            _call_processes(args, command_list, None, outfile)
    else:
        # Call processes with no redirection
        _call_processes(args, command_list, None, sys.stdout)


def _split_list(items, sep):
    sublist = []
    for elem in items:
        if elem == sep:
            yield list(sublist)
            sublist = []
        else:
            sublist.append(elem)
    yield list(sublist)


def _get_next(items, identifier):
    for elem, nextelem in zip(items,items[1:]):
        if elem == identifier:
            return nextelem

def _call_processes(args, command_list, infile, outfile):

    # List of processes created
    processes = []

    if len(command_list) == 1:

        # Create a single process with no pipeing
        processes.append(_call_process(command_list[0], stdin=infile, stdout=outfile, stderr=sys.stderr))

    else:

        # Create the first process with stdin from infile
        processes.append(_call_process(command_list[0], stdin=infile, stdout=subprocess.PIPE, stderr=sys.stderr))

        # Create intermediate processes with pipes
        for command in command_list[1:-1]:
            processes.append(_call_process(command, stdin=processes[-1].stdout, stdout=subprocess.PIPE, stderr=sys.stderr))
            processes[-2].stdout.close()

        # Create final process with stdout as outfile
        processes.append(_call_process(command_list[-1], stdin=processes[-1].stdout, stdout=outfile, stderr=sys.stderr))
        processes[-2].stdout.close()

    # Wait for all process
    for process in processes:
        process.wait()

    # Check return codes
    for idx, process in enumerate(processes):
        if process.returncode != 0:
            raise CommandLineException(args, command_list[idx][0], process.returncode)

def _call_process(command, stdin, stdout, stderr):
    try:
        return subprocess.Popen(command, stdin=stdin, stdout=stdout, stderr=stderr)
    except OSError as e:
        if e.errno == 2:
            raise CommandNotFoundException(command, command[0])
        else:
            raise e
