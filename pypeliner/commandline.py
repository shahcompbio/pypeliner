import sys
import os
import logging
import subprocess
from pypeliner.helpers import Backoff
import pypeliner

def which(file):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, file)) and os.path.isfile(os.path.join(path, file)):
            return os.path.join(path, file)
    return None


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
        return "Command '%s' with return code %d in command line `%s`" % (
            self.command, self.returncode, " ".join(self.args))


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

    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        self.retval = self.func(*self.args, **self.kwargs)


def execute(*args, **docker_kwargs):
    """ Execute a command line

    :param args: executable and command line arguments
    :param kwargs: container keyword arguments

    Execute a command line, and handle pipes between processes and to files.
    The '|', '>' and '<' characters are interpretted as pipes between processes,
    to files and from files in the same way as in bash.  Each process is checked
    for successful completion, with a meaningful exception thrown in the case of
    an error.

    :raises: :py:class:`CommandLineException`, :py:class:`CommandNotFoundException`

    """
    if docker_kwargs and docker_kwargs.get("container_type", None) == 'docker':
        args = dockerize_args(*args, **docker_kwargs)
        _docker_pull(docker_kwargs.get("image"),
                     docker_kwargs.get("server"),
                     docker_kwargs.get("username"),
                     docker_kwargs.get("password"),
                     )

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
            processes.append(
                _call_process(command, stdin=processes[-1].stdout, stdout=subprocess.PIPE, stderr=sys.stderr))
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


def singularity_args(*args, **kwargs):
    image = kwargs.get("docker_image")
    assert image, "singularity image path required."
    mounts = sorted(set(kwargs.get("mounts", {}).values()))
    docker_args = ['singularity', 'run']
    for mount in mounts:
        docker_args.extend(['-B', '{}:{}'.format(mount, mount)])
    # paths on azure are relative, so we need to set the working dir
    wdir = os.getcwd()
    docker_args.extend(['--pwd', wdir])
    docker_args.append(image)
    args = docker_args + list(args)
    return args


def dockerize_args(*args, **kwargs):
    if kwargs.get("no_container"):
        return args
    image = kwargs.get("docker_image")

    kwargs = pypeliner.helpers.GlobalState.get("context_config")

    if not kwargs or not kwargs.get("docker"):
        return args

    if not image:
        logging.getLogger('pypeliner.commandline').warn('running locally, no docker image specified')
        return args

    server = kwargs['docker']['server']
    image = server + '/' + image

    kwargs = kwargs.get('docker', None)

    mounts = sorted(set(kwargs.get("mounts", {}).values()))

    docker_args = ['docker', 'run']
    for mount in mounts:
        docker_args.extend(['-v', '{}:{}'.format(mount, mount)])

    # paths on azure are relative, so we need to set the working dir
    wdir = os.getcwd()
    docker_args.extend(['-w', wdir])
    # remove container after it finishes running
    docker_args.append('--rm')

    docker_path = which('docker')
    if not docker_path:
        raise Exception("Couldn't find docker in system")

    # expose docker socket to enable starting
    # new containers from the current container
    docker_args.extend(['-v', '/var/run/docker.sock:/var/run/docker.sock'])
    docker_args.extend(['-v', '{}:/usr/bin/docker'.format(docker_path)])
    docker_args.append(image)

    args = list(args)
    args = ['bash', '-e', '-c'] + [' '.join(map(str, args))]

    args = docker_args + args

    _docker_pull(
        image, server, kwargs['username'], kwargs['password']
    )

    return args


def _docker_pull(image, server, username, password):
    cmd = ['docker','pull',image]

    # log in if pull fails
    try:
        execute(*cmd)
    except CommandLineException:
        _docker_login(server, username, password)
        execute(*cmd)


@Backoff(max_backoff=300, randomize=True)
def _docker_login(server, username, password):
    if not username or not password:
        # assume repo is public
        return

    cmd = ['echo', password, '|', 'docker', 'login',
           server, '-u', username, '--password-stdin',
           '>', '/dev/null']

    execute(*cmd)
