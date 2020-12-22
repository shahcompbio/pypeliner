import logging
import os
import uuid
from os.path import expanduser

import pypeliner


def which(filepath):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, filepath)) and os.path.isfile(os.path.join(path, filepath)):
            return os.path.join(path, filepath)
    return None


def get_shell_file_path():
    filename = str(uuid.uuid1())
    tempdir = pypeliner.helpers.GlobalState.get('tmpdir')
    if not tempdir:
        raise Exception()
    shell_file = os.path.join(tempdir, 'shell_scripts', "{}.sh".format(filename))
    return shell_file


def containerize_args(*args, **kwargs):
    if kwargs.get("no_container"):
        return args, []
    docker_image = kwargs.get("docker_image")

    context_cfg = pypeliner.helpers.GlobalState.get("context_config")

    execute = kwargs.get("execute")

    shell_files = []

    if context_cfg and context_cfg.get('singularity'):
        args, shell_files = singularity_args(args, docker_image, context_cfg, execute)
    elif context_cfg and context_cfg.get('docker'):
        args, shell_files = dockerize_args(args, docker_image, context_cfg,)

    return args, shell_files


def singularity_args(args, image, context_cfg, execute):
    shell_files = []

    if not image:
        logging.getLogger('pypeliner.commandline').warn(
            'running locally, no docker image specified'
        )
        return args, []

    singularity = context_cfg['singularity'].get('singularity_exe', 'singularity')

    server = context_cfg['singularity']['server']
    image = 'docker://' + server + '/' + image

    kwargs = context_cfg.get('singularity', None)

    commands = []

    local_path = kwargs.get('local_cache')
    if local_path:
        commands.append(['export', 'SINGULARITY_CACHEDIR={}'.format(local_path)])

    username = kwargs.get('username')
    if username:
        commands.append(['export', 'SINGULARITY_DOCKER_USERNAME={}'.format(username)])

    password = kwargs.get('password')
    if password:
        commands.append(['export', 'SINGULARITY_DOCKER_PASSWORD={}'.format(password)])

    singularity_command = [singularity, 'run']

    mounts = sorted(set(kwargs.get("mounts", {}).values()))
    for mount in mounts:
        singularity_command.extend(['--bind', mount])
    # paths on azure are relative, so we need to set the working dir
    wdir = os.getcwd()
    singularity_command.extend(['--pwd', wdir])
    singularity_command.append(image)

    if '|' in args or '>' in args or '<' in args:
        shell_file = write_to_shell_script([args])
        shell_files.append(shell_file)
        args = ['bash', shell_file]

    singularity_command.extend(args)

    commands.append(singularity_command)

    shell_file = write_to_shell_script(commands)
    shell_files.append(shell_file)

    command = ['bash', os.path.abspath(shell_file)]

    if execute:
        ssh_localhost = [
            'ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', 'localhost'
        ]
        command = ssh_localhost + command

    return command, shell_files


def dockerize_args(args, image, context_cfg):
    shell_files = []

    if not image:
        logging.getLogger('pypeliner.commandline').warn(
            'running locally, no docker image specified'
        )
        return args, shell_files

    server = context_cfg['docker'].get('server')
    kwargs = context_cfg.get('docker', None)

    docker_prep_command = None
    if server is not None:
        docker_prep_command = get_docker_prep_command(
            server, image, kwargs['username'], kwargs['password']
        )

    mounts = sorted(set(kwargs.get("mounts", {}).values()))

    docker_args = ['udocker', 'run']
    for mount in mounts:
        docker_args.extend(['-v', '{}:{}'.format(mount, mount)])

    # paths on azure are relative, so we need to set the working dir
    wdir = os.getcwd()
    # docker_args.extend(['-w', wdir])
    docker_args.extend(['-w', '$PWD'])

    # map working directory into the container
    docker_args.extend(['-v', '$PWD:$PWD'])

    # remove container after it finishes running
    docker_args.append('--rm')

    docker_path = which('udocker')
    if not docker_path:
        raise Exception("Couldn't find docker in system")
    docker_args.extend(['-v', '{}:/usr/bin/docker'.format(docker_path)])

    if 'ROOT_HOME' in os.environ:
        mount_path = os.environ['ROOT_HOME']
    else:
        mount_path = expanduser('~')
    # will copy config file which preserves docker login
    # all containers after  the first one will run as root
    docker_args.extend(['-e', 'ROOT_HOME={}'.format(mount_path)])

    image_uri = image
    if server is not None:
        image_uri = server + '/' + image
    docker_args.append(image_uri)

    if '|' in args or '>' in args or '<' in args:
        shell_file = write_to_shell_script([args])
        shell_files.append(shell_file)
        args = ['bash', shell_file]

    args = docker_args + list(args)

    if docker_prep_command is not None:
        cmds = [docker_prep_command, args]
    else:
        cmds = [args]

    shell_file = write_to_shell_script(cmds)
    shell_files.append(shell_file)

    return ['bash', shell_file], shell_files


def get_docker_prep_command(server, image, username, password):
    # try to pull the image. if fails then login and retry
    image_uri = server + '/' + image

    pull_cmd = 'udocker pull {0}'.format(image_uri)
    login_cmd = 'udocker login {0} -u {1} -p {2}'.format(server, username, password)

    cmd = ['{\n', pull_cmd, '\n}', '||', '{\n', login_cmd, '&&', pull_cmd, '\n}']
    cmd = ' '.join(cmd)

    return cmd


def write_to_shell_script(commands, shell_file=None):
    if not shell_file:
        shell_file = get_shell_file_path()

    pypeliner.helpers.makedirs(os.path.dirname(shell_file))

    with open(shell_file, 'w') as shell_output:
        shell_output.write('#!/bin/bash\n')
        shell_output.write('set -e\n')
        shell_output.write('set -o pipefail\n\n')

        for command in commands:
            if isinstance(command, list) or isinstance(command, tuple):
                command = map(str, command)
                command = ' '.join(command)

            shell_output.write(command + '\n')

    return shell_file
