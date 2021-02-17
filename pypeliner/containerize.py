import os

import pypeliner


def containerize_args(*args):
    context_cfg = pypeliner.helpers.GlobalState.get("context_config")

    if not context_cfg:
        return args
    elif context_cfg.get('singularity'):
        return singularity_args(args, context_cfg['singularity'])
    elif context_cfg.get('docker'):
        return dockerize_args(args, context_cfg['docker'])
    else:
        return args


def singularity_args(args, context_cfg):
    singularity = context_cfg.get('singularity_exe', 'singularity')
    image = context_cfg['image']

    if not image.endswith('.sif'):
        raise Exception('only sif images are allowed')

    singularity_command = [singularity, 'run']

    mounts = sorted(set(context_cfg.get("mounts", {}).values()))
    for mount in mounts:
        singularity_command.extend(['--bind', mount])

    wdir = os.getcwd()
    singularity_command.extend(['--pwd', wdir])
    singularity_command.append(image)

    singularity_command.extend(args)

    return singularity_command


def dockerize_args(args, context_cfg):
    docker_args = [
        'docker', 'run', '-w', '$PWD', '-v', '$PWD:$PWD',
        '--rm'
    ]

    mounts = sorted(set(context_cfg.get("mounts", {}).values()))
    for mount in mounts:
        docker_args.extend(['-v', '{}:{}'.format(mount, mount)])

    docker_args.append(context_cfg['image'])

    args = docker_args + list(args)

    return args
