import subprocess
import sys
import os
import dill as pickle
import pypeliner

class CommandLineException(Exception):
    """ A command produced a non-zero exit code.

    :param args: full set of arguments in failed command line
    :param command: command that failed
    :param returncode: exit code of failed command

    """
    def __init__(self, args, command, returncode):
        self.args = args
        self.command = command
        self.returncode = returncode
    def __str__(self):
        return "Command '%s' with return code %d in command line `%s`" % (self.command, self.returncode, " ".join(self.args))

class CommandNotFoundException(Exception):
    """ A command was not found on the path.

    :param args: full set of arguments in failed command line
    :param command: command that could not be found

    """
    def __init__(self, args, command):
        self.args = args
        self.command = command
    def __str__(self):
        return "Command '%s' not found in command line `%s`" % (self.command, " ".join(self.args))


class Callable(object):
    """callable functions and args for pypeliner_delegate
    for running in docker containers

    :param func: function to run in docker
    :param args: arguments

    """
    def __init__(self, func, args):
        self.func = func
        self.args = args
    def __call__(self):
        self.retval = self.func(*self.args)


def docker_python_execute(*args, **kwargs):
    """ Run a python function in docker

    :param args: executable and command line arguments
    :param kwargs: function, tempdir and docker image

    Execute python code inside a docker container. Pickles the code
    as a callable and runs with pypeliner_delegate

    """
    args = _update_args_to_abspath(args)

    func = kwargs["func"]
    if isinstance(func, str):
        func = pypeliner.helpers.import_function(func)
    jobcallable = Callable(func, args)

    tempdir = kwargs["tempdir"]
    pypeliner.helpers.makedirs(tempdir)
    before = os.path.join(tempdir, 'before.pickle')
    after = os.path.join(tempdir, 'after.pickle')
    docker_args = ["pypeliner_delegate", before, after]
    if kwargs.get('dockerize'):
        docker_args = _dockerize_args(
            *docker_args, image=kwargs.get("image", None),
            mounts=_get_mount_dirs(args))

    with open(before, 'w') as beforepickle:
        pickle.dump(jobcallable, beforepickle)

    execute(*docker_args)

    with open(after) as afterpickle:
        data = pickle.load(afterpickle)
        return data.retval


def execute(*args, **kwargs):
    """ Execute a command line

    :param args: executable and command line arguments
    :param kwargs: dockerize keyword argument

    Execute a command line, and handle pipes between processes and to files.
    The '|', '>' and '<' characters are interpretted as pipes between processes,
    to files and from files in the same way as in bash.  Each process is checked
    for successful completion, with a meaningful exception thrown in the case of
    an error.

    :raises: :py:class:`CommandLineException`, :py:class:`CommandNotFoundException`

    """
    if kwargs and kwargs.get("dockerize", None):
        args = _dockerize_args(*args, **kwargs)

    if args.count(">") > 1 or args[0] == ">" or args[-1] == ">":
        raise ValueError("Bad redirect to file")

    if args.count("<") > 1 or args[0] == "<" or args[-1] == "<":
        raise ValueError("Bad redirect from file")

    args = filter(lambda arg: arg != '', args)
    args = list(str(arg) for arg in args)
    command_list = list(_split_list(args, "|"))

    # Determine input filename for redirect if any
    input = None
    if command_list[0].count("<") == 1:
        input = _get_next(command_list[0], "<")
        command_list[0].remove("<")
        command_list[0].remove(input)

    # Determine output filename for redirect if any
    output = None
    if command_list[-1].count(">") == 1:
        output = _get_next(command_list[-1], ">")
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
        # Call processes with output
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
    for elem, nextelem in zip(items, items[1:]):
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
            raise

def _update_args_to_abspath(args):

    updated_args = []

    for arg in args:
        if isinstance(arg, str) and os.path.exists(os.path.dirname(arg)):
            arg = os.path.abspath(arg)
            updated_args.append(arg)
        else:
            updated_args.append(arg)

    return updated_args


def _get_mount_dirs(args):
    mounts = set()

    for arg in args:
        if not isinstance(arg, str):
            continue
        if os.path.exists(os.path.dirname(arg)):
            if not arg.startswith('/'):
                arg = os.path.abspath(arg)
            arg = arg.split('/')

            mounts.add('/' + arg[1])

    return sorted(mounts)

def _dockerize_args(*args, **kwargs):
    image = kwargs.get("image")
    assert image, "docker image URL required."

    args = _update_args_to_abspath(args)

    mounts = _get_mount_dirs(args)

    if kwargs.get('mounts', None):
        mounts += kwargs.get('mounts')
        mounts = sorted(set(mounts))

    docker_args = ['docker', 'run']

    for mount in mounts:
        docker_args.extend(['-v', '{}:{}'.format(mount, mount)])

    # expose docker socket to enable starting
    # new containers from the current container
    docker_args.extend(['-v', '/var/run/docker.sock:/var/run/docker.sock'])
    docker_args.extend(['-v', '/usr/bin/docker:/usr/bin/docker'])

    docker_args.append(image)
    args = docker_args + args

    return args

