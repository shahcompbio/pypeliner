import importlib
import pypeliner.execqueue.local


def create(requested_queue, modules=None, native_spec=None, config_filename=None):
    if requested_queue is None:
        raise Exception('No submit queue specified')
    elif requested_queue == 'local':
        exec_queue_name = 'pypeliner.execqueue.local.LocalJobQueue'
    elif requested_queue == 'qsub':
        exec_queue_name = 'pypeliner.execqueue.qsub.QsubJobQueue'
    elif requested_queue == 'asyncqsub':
        exec_queue_name = 'pypeliner.execqueue.qsub.AsyncQsubJobQueue'
    elif requested_queue == 'lsf':
        exec_queue_name = 'pypeliner.execqueue.qsub.LsfJobQueue'
    elif requested_queue == 'slurm':
        exec_queue_name = 'pypeliner.execqueue.qsub.SlurmJobQueue'
    elif requested_queue == 'pbs':
        exec_queue_name = 'pypeliner.execqueue.qsub.PbsJobQueue'
    elif requested_queue == 'drmaa':
        exec_queue_name = 'pypeliner.execqueue.drmaa.DrmaaJobQueue'
    elif requested_queue == 'azurebatch':
        exec_queue_name = 'pypeliner.contrib.azure.batchqueue.AzureJobQueue'
    elif requested_queue == 'awsbatch':
        exec_queue_name = 'pypeliner.contrib.aws.batchqueue.AwsJobQueue'
    else:
        exec_queue_name = requested_queue

    exec_queue_class_name = exec_queue_name.split('.')[-1]
    exec_queue_module_name = exec_queue_name[:-len(exec_queue_class_name)-1]

    exec_queue_module = importlib.import_module(exec_queue_module_name)
    exec_queue_class = vars(exec_queue_module)[exec_queue_class_name]

    exec_queue = exec_queue_class(modules=modules, native_spec=native_spec, config_filename=config_filename)

    if requested_queue != 'local':
        exec_queue = pypeliner.execqueue.local.LocalRemoteQueue(exec_queue, modules=modules)

    return exec_queue
