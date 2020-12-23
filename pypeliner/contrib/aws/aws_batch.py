import os
import boto3
import time


class AwsBatch(object):
    def __init__(self):
        self.batch_client = boto3.client('batch')
        self.log_client = boto3.client('logs')

    def register_job_definition(self, defn_name, container_prop, retry_strategy, container_type):
        """
        register a job definition with aws batch
        :param defn_name: name for the job def
        :type defn_name: str
        :param container_prop: container properties for job def
        :type container_prop: dict
        :param retry_strategy: number of retries allowed
        :type retry_strategy: dict
        :param container_type: type of container (container|Multinode)
        :type container_type: str
        :return: ARN for the registered job def
        :rtype: str
        """
        response = self.batch_client.register_job_definition(
            jobDefinitionName=defn_name,
            type=container_type,
            containerProperties=container_prop,
            retryStrategy=retry_strategy,
        )
        return response['jobDefinitionArn']

    def batch_compatible_format(self, string):
        """
        for string to make it compatible with AWS batch naming conventions
        :param string: input string
        :type string: str
        :return: formatted string
        :rtype: str
        """
        string = string.replace('/', '_')
        string = string.replace(':', '_')
        string = string.replace('.', '')

        if string.startswith('_'):
            string = string[1:]
        return string

    def _compare_dicts(self, ref1, ref2):
        """
        compares 2 dictionaries
        :param ref1: first dict to compare
        :type ref1: dict
        :param ref2: second dict to compare
        :type ref2: dict
        :return: True if the dicts are same
        :rtype: bool
        """
        keys = set(list(ref1.keys()) + list(ref2.keys()))

        for key in keys:
            ref1val = ref1.get(key, None)
            ref2val = ref2.get(key, None)

            # also ignores empty lists. for instance if ulimits are not specified
            # when registering a job then aws batch returns [] for those defns.
            if not ref1val and not ref2val:
                continue

            if isinstance(ref1val, dict) and isinstance(ref2val, dict):
                result = self._compare_dicts(ref1val, ref2val)
                if not result:
                    return False

            if not ref1val == ref2val:
                return False

        return True

    @staticmethod
    def _get_boto3_iterator(func, kwargs, response_key):
        """
        utility function to get an iterator over batch list functions
        :param func: batch function to iterate over
        :type func: function
        :param kwargs: arguments for the func
        :type kwargs: dict
        :param response_key: key-value pair in aws response to iterate over
        :type response_key: str
        :return: values for the response key
        :rtype: generator
        """
        def run_func(next_token):
            if next_token:
                return func(**dict(nextToken=next_token, **kwargs))
            else:
                return func(**kwargs)

        next_token=None
        while True:
            response = run_func(next_token)

            next_token = response.get('nextToken', None)

            for value in response[response_key]:
                yield value
            if not next_token:
                break

    def _search_matching_job_defn(self, retry_strategy, container_properties, container_type):
        """
        look through all revisions of existing job definitions and return
        the revision that matches the request.
        :param retry_strategy: requested retry strategy
        :type retry_strategy: dict
        :param container_properties: requested conatiner properties
        :type container_properties: dict
        :param container_type: type (container | multinode)
        :type container_type: str
        :return: matching job definition
        :rtype: str
        """
        job_defns = self._get_boto3_iterator(
            self.batch_client.describe_job_definitions,
            {'maxResults': 10, 'status': 'ACTIVE'},
            'jobDefinitions'
        )

        for job_defn in job_defns:
            defn_container_props = job_defn['containerProperties']
            defn_retry = job_defn.get('retryStrategy',0)
            defn_container_type = job_defn['type']
            defn_arn = job_defn['jobDefinitionArn']

            match = True
            if not defn_retry == retry_strategy:
                match = False
            if not self._compare_dicts(defn_container_props, container_properties):
                match = False
            if not defn_container_type == container_type:
                match = False
            if match:
                return defn_arn
        return

    def get_matching_job_defn(self, config, ctx, context_config):
        """
        build kwargs to request new job defn, check existing revisions
        to see if there is matching one already. if not then create a new one
        :param config: AWS batch submit config
        :type config: dict
        :param ctx: job context
        :type ctx: dict
        :param context_config: pypeliner context config
        :type context_config: dict
        :return: job definition ARN
        :rtype:  str
        """
        image_name = ctx.get("docker_image")
        image = os.path.join(context_config['docker']['server'], image_name)
        defname = self.batch_compatible_format(image_name)
        def_metadata = config['definition']

        mount_config = context_config['docker']['mounts']
        # required for docker inside docker
        mount_config['docker_socket'] = '/var/run/docker.sock'
        mount_config['docker'] = '/usr/bin/docker'

        volumes = []
        for name, path in mount_config.items():
            volumes.append({'name': name, 'host': {'sourcePath': path}})

        mountpoints = []
        for name, path in mount_config.items():
            mountpoints.append({'containerPath': path, 'sourceVolume': name, 'readOnly': False})

        env_vars = []
        if def_metadata['env_vars']:
            for name, value in def_metadata['env_vars'].items():
                env_vars.append({'name': name, 'value': value})

        container_properties = {
            'image': image,
            'vcpus': def_metadata['vcpus'],
            'memory': def_metadata['mem'],
            'command': def_metadata['command'],
            'jobRoleArn': def_metadata['job_role_arn'],
            'volumes': volumes,
            'environment': env_vars,
            'mountPoints': mountpoints,
            'readonlyRootFilesystem': def_metadata['readonlyRootFilesystem'],
            'privileged': def_metadata['privileged'],
        }

        retry_strategy = {'attempts': 1}

        matching_job_def = self._search_matching_job_defn(
            container_properties, retry_strategy, def_metadata['type']
        )
        if not matching_job_def:
            matching_job_def = self.register_job_definition(
                defname, container_properties,
                retry_strategy, def_metadata['type']
            )
        return matching_job_def

    def list_all_jobs(self, config, run_id, status=None):
        """
        lists all jobs for specified run id
        :param config: pypeliner batch submit config
        :type config: dict
        :param run_id: pipeline run id
        :type run_id: str
        :return: summaries of all jobs
        :rtype: list
        """
        queues = []
        config = config.get("job_queues")
        spot_queue = config["spot"]
        queues.append('{}_{}'.format(spot_queue['queue_name_prefix'], run_id))
        dedicated_queue = config["dedicated"]
        queues.append('{}_{}'.format(dedicated_queue['queue_name_prefix'], run_id))
        return self.list_jobs(queues, status=status)


    def list_jobs(self, job_queues, status=None):
        """
        list all jobs
        :param job_queues: job queues to look for jobs in
        :type job_queues: list of str
        :param status: only list jobs with this status
        :type status: list of str
        :return: job summaries for matching jobs
        :rtype: generator
        """
        valid_statuses = ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING',
                          'RUNNING', 'SUCCEEDED', 'FAILED']
        if not status:
            status = valid_statuses

        for stat in status:

            for job_queue in job_queues:

                assert stat in valid_statuses

                jobs = self._get_boto3_iterator(
                    self.batch_client.list_jobs,
                    {'jobQueue': job_queue, 'jobStatus': stat, 'maxResults': 10},
                    'jobSummaryList'
                )

                for job in jobs:
                    yield job

    def get_compute_environment_status(self, env_name):
        """
        get status of compute environments
        :return: names of all compute environments
        :rtype: list of str
        """
        env = self._get_boto3_iterator(
            self.batch_client.describe_compute_environments,
            {'computeEnvironments': [env_name]},
            'computeEnvironments'
        )
        env = list(env)
        assert len(env) == 1
        env = env[0]
        return env.get("status")

    def get_job_queue_status(self, queue_name):
        """
        get status of compute environments
        :return: names of all compute environments
        :rtype: list of str
        """
        queue = self._get_boto3_iterator(
            self.batch_client.describe_job_queues,
            {'jobQueues': [queue_name]},
            'jobQueues'
        )
        queue = list(queue)
        assert len(queue) == 1
        queue = queue[0]
        return queue.get("status")

    def list_compute_environments(self):
        """
        list all existing compute environments
        :return: names of all compute environments
        :rtype: list of str
        """
        envs = self._get_boto3_iterator(
            self.batch_client.describe_compute_environments,
            {'maxResults': 10},
            'computeEnvironments'
        )
        envs = [env['computeEnvironmentName'] for env in envs]
        return envs

    def list_job_queues(self):
        """
        lists all job queues
        :return: names of all job queues
        :rtype: list of str
        """
        envs = self._get_boto3_iterator(
            self.batch_client.describe_job_queues,
            {'maxResults': 10},
            'jobQueues'
        )
        envs = [env['jobQueueName'] for env in envs]
        return envs

    def create_compute_environment(self, env_name, env_params):
        """
        create a new compute environment
        :param env_name: name of env
        :type env_name: str
        :param env_params: params from submit config
        :type env_params: dict
        """
        compute_resources = {
           'type': env_params['type'],
           'minvCpus': env_params['minvCpus'],
           'maxvCpus': env_params['maxvCpus'],
           'desiredvCpus': env_params['desiredvCpus'],
           'instanceTypes': env_params['instanceTypes'],
           'imageId': env_params['imageId'],
           'subnets': env_params['subnets'],
           'securityGroupIds': env_params['securityGroupIds'],
           'ec2KeyPair': env_params['ec2KeyPair'],
           'instanceRole': env_params['instanceRole'],
           'tags': env_params['tags'],
           'placementGroup': env_params['placementGroup'],
           'bidPercentage': env_params['bidPercentage'],
           'spotIamFleetRole': env_params['spotIamFleetRole'],
           'launchTemplate': env_params['launchTemplate']
        }

        # boto3 doesnt accept None
        pop_keys = [k for k, v in compute_resources.items() if v is None]
        [compute_resources.pop(k) for k in pop_keys]

        response = self.batch_client.create_compute_environment(
            computeEnvironmentName=env_name,
            type='MANAGED',
            state='ENABLED',
            computeResources=compute_resources,
            serviceRole=env_params['serviceRole']
        )

    def create_compute_environments(self, config):
        """
        create all compute environments in submit config
        if required
        :param config: aws batch submit configuration data
        :type config: dict
        """
        existing_envs = self.list_compute_environments()

        config = config.get("compute_environments")
        if not config:
            return

        for env_name, env_params in config.items():
            if env_name in existing_envs:
                continue
            self.create_compute_environment(env_name, env_params)

    def create_job_queue(self, queue_name, queue_params):
        """
        create a job queue
        :param queue_name:  name of the queue
        :type queue_name: str
        :param queue_params: queue params from submit config
        :type queue_params: dict
        """
        response = self.batch_client.create_job_queue(
            jobQueueName=queue_name,
            state='ENABLED',
            priority=queue_params['priority'],
            computeEnvironmentOrder=[
                {
                    'order': 1,
                    'computeEnvironment': queue_params['computeEnvironment']
                },
            ]
        )

    def create_job_queues(self, config, run_id):
        """
        create all job queues in submit config
        if required
        :param run_id: pipeline run id
        :type run_id: str
        :param config: aws batch submit configuration data
        :type config: dict
        """
        config = config.get("job_queues")
        existing_queues = self.list_job_queues()

        spot_queue = config["spot"]
        queue_name = '{}_{}'.format(spot_queue['queue_name_prefix'], run_id)
        if queue_name not in existing_queues:
            self.create_job_queue(queue_name, spot_queue)

        dedicated_queue = config["dedicated"]
        queue_name = '{}_{}'.format(dedicated_queue['queue_name_prefix'], run_id)
        if queue_name not in existing_queues:
            self.create_job_queue(queue_name, dedicated_queue)

    def get_log_stream_name(self, job_id):
        """
        get the logstream name for the job.
        :param job_id: job Id
        :type job_id: str
        :return: log stream name
        :rtype:  str
        """
        desc = self.batch_client.describe_jobs(jobs=[job_id])
        assert len(desc['jobs']) == 1
        desc = desc['jobs'][0]
        return desc['container']['logStreamName']

    def get_cloudwatch_error(self, job_id):
        """
        fetch all logs for the job
        :param job_id: job Id
        :type job_id: str
        :return: logs for the job
        :rtype: str
        """
        logstream = self.get_log_stream_name(job_id)

        kwargs = {
            'logGroupName': '/aws/batch/job',
            'logStreamName': logstream,
            'limit': 100,
            'startFromHead': True
        }


        logs = self._get_boto3_iterator(
            self.log_client.get_log_events,
            kwargs,
            'events'
        )

        data = [event.message for event in logs]

        return '\n'.join(data)

    def get_job_exitcode(self, job_id):
        """
        get exit code from aws batch
        :param job_id: job Id
        :type job_id: str
        :return: exit code
        :rtype: int
        """
        desc = self.batch_client.describe_jobs(jobs=[job_id])

        assert len(desc['jobs']) == 1

        status = desc['jobs'][0]['status']

        assert status in ['SUCCEEDED', 'FAILED'], "job is not in finished state"

        attempts = desc['jobs'][0]['attempts']

        attempt_codes = [attempt['container']['exitCode'] for attempt in attempts]

        # exit code should be 0 if job succeeded.
        if not status == 'FAILED':
            assert list(set(attempt_codes)) == [0]

        # only reporting the last exit code if multiple attempts
        exitcode = attempt_codes[-1]
        return exitcode

    def terminate_job(self, job_id, reason):
        """
        kill a job
        :param job_id: job Id
        :type job_id: str
        :param reason: reason for killing
        :type reason: str
        """
        self.batch_client.terminate_job(jobId=job_id, reason=reason)

    def submit_job(self, job_name, defn_id, ctx, command, config, run_id):
        """
        submit a job
        :param job_name: unique job name
        :type job_name: str
        :param defn_id: job definition name or ARN
        :type defn_id: str
        :param ctx: job context
        :type ctx: dict
        :param command: job command
        :type command:  list
        :param config: batch submit config
        :param run_id: pipeline run id
        :type run_id: str
        :type config: dict
        :return: job Id
        :rtype: str
        """
        if ctx.get("dedicated"):
            jobqueue = config['job_queues']['dedicated']['queue_name_prefix']
        else:
            jobqueue = config['job_queues']['spot']['queue_name_prefix']
        jobqueue = "{}_{}".format(jobqueue, run_id)
        response = self.batch_client.submit_job(
            jobName=job_name,
            jobQueue=jobqueue,
            jobDefinition=defn_id,
            containerOverrides={
                'vcpus': ctx.get('ncpus'),
                'memory': ctx.get('mem') * 1024,
                'command': command,
            }
        )

        job_id = response['jobId']

        return job_id

    def delete_job_queue(self, job_queue):
        """
        delete a job queue
        :param job_queue: job queue name
        :type job_queue: str
        """
        self.batch_client.delete_job_queue(
            jobQueue=job_queue
        )

        while job_queue in self.list_job_queues():
            time.sleep(10)

    def disable_job_queue(self, job_queue):
        """
        disable a job queue
        :param job_queue: job queue name
        :type job_queue: str
        """

        self.batch_client.update_job_queue(
            jobQueue=job_queue,
            state='DISABLED'
        )

        while True:
            status = self.get_job_queue_status(job_queue)
            if not status == 'UPDATING':
                break
            time.sleep(10)

    def delete_compute_environment(self, compute_environment):
        """
        delete a compute environment
        :param compute_environment: compute environment name
        :type compute_environment: str
        """
        self.batch_client.delete_compute_environment(
            computeEnvironment=compute_environment
        )

        while compute_environment in self.list_compute_environments():
            time.sleep(10)

    def disable_compute_environment(self, compute_environment):
        """
        disable a compute environment
        :param compute_environment: compute environment name
        :type compute_environment: str
        """
        self.batch_client.update_compute_environment(
            computeEnvironment=compute_environment,
            state='DISABLED'
        )

        while True:
            status = self.get_compute_environment_status(compute_environment)
            if not status == 'UPDATING':
                break
            time.sleep(10)

    def delete_job_queues(self, config, run_id):
        """
        delete all job queues
        :param config: pypeliner submit config
        :type config: dict
        :param run_id: pipeline run id
        :type run_id: str
        """
        if not config.get('delete_job_queues'):
            return

        config = config.get("job_queues")

        spot_queue = config["spot"]
        queue_name = '{}_{}'.format(spot_queue['queue_name_prefix'], run_id)
        self.disable_job_queue(queue_name)
        self.delete_job_queue(queue_name)

        dedicated_queue = config["dedicated"]
        queue_name = '{}_{}'.format(dedicated_queue['queue_name_prefix'], run_id)
        self.disable_job_queue(queue_name)
        self.delete_job_queue(queue_name)

    def delete_compute_environments(self, config):
        """
        delete all compute environments
        :param config: pypeliner submit config
        :type config: dict
        """
        if not config.get('delete_compute_environments'):
            return

        config = config.get("compute_environments")

        for env_name, env_params in config.items():
            self.disable_compute_environment(env_name)
            self.delete_compute_environment(env_name)

