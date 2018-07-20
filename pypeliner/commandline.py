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
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
    def __call__(self):
        self.retval = self.func(*self.args, **self.kwargs)


def _docker_python_execute(args, kwargs, ctx, func, mounts, tempdir):
    """ Run a python function in docker

    :param args: executable and command line arguments
    :param kwargs: function, tempdir and docker image

    Execute python code inside a docker container. Pickles the code
    as a callable and runs with pypeliner_delegate

    """
    args = _update_args_to_abspath(args)

    if isinstance(func, str):
        func = pypeliner.helpers.import_function(func)
    jobcallable = Callable(func, args, kwargs)

    pypeliner.helpers.makedirs(tempdir)
    before = os.path.join(tempdir, 'before.pickle')
    after = os.path.join(tempdir, 'after.pickle')
    docker_args = ["pypeliner_delegate", before, after]
    if ctx.get("dockerize"):
        docker_args = _dockerize_args(
            *docker_args, image=ctx.get("image"),
            mounts=mounts
            )

    with open(before, 'w') as beforepickle:
        pickle.dump(jobcallable, beforepickle)

    _docker_login(ctx.get('server'),
                  ctx.get("username"),
                  ctx.get("password"),)
    _docker_pull(ctx.get("image"))
    execute(*docker_args)

    with open(after) as afterpickle:
        data = pickle.load(afterpickle)
        return data.retval


def execute(*args, **docker_kwargs):
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
    if docker_kwargs and docker_kwargs.get("dockerize", None):
        args = _dockerize_args(*args, **docker_kwargs)
        _docker_login(docker_kwargs.get("server"),
                      docker_kwargs.get("username"),
                      docker_kwargs.get("password"),)
        _docker_pull(docker_kwargs.get("image"))


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


def _update_str_to_abspath(arg):
    if isinstance(arg, str) and os.path.exists(os.path.dirname(arg)):
        return os.path.abspath(arg)
    else:
        return arg

def _update_dict_to_abspath(arg):
    outarg = {}
    for key, val in arg.iteritems():
        if isinstance(val, list) or isinstance(val, tuple):
            val = _update_args_to_abspath(val)
        elif isinstance(val, dict):
            val = _update_dict_to_abspath(val)
        elif isinstance(val, str):
            val = _update_str_to_abspath(val)

        outarg[key] = val

    return outarg

def _update_args_to_abspath(args):
    updated_args = []

    for arg in args:
        if isinstance(arg, list) or isinstance(arg, tuple):
            arg = _update_args_to_abspath(arg)
        elif isinstance(arg, dict):
            arg = _update_dict_to_abspath(arg)
        elif isinstance(arg, str):
            arg = _update_str_to_abspath(arg)

        updated_args.append(arg)

    return updated_args

def _dockerize_args(*args, **kwargs):
    image = kwargs.get("image")
    assert image, "docker image URL required."

    args = _update_args_to_abspath(args)

    mounts = kwargs.get("mounts", [])

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


def _docker_pull(image):
    cmd = ['docker','pull',image]
    execute(*cmd)

def _docker_login(server, username, password):
    if not username or not password:
        #assume repo is public
        return
    cmd = ['docker', 'login', server, '-u', username, '-p', password]
    execute(*cmd)