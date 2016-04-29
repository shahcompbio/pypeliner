

class JobQueue(object):
    """ Abstract class for a queue of jobs.
    """
    def send(self, ctx, name, sent, temps_dir):
        """ Add a job to the queue.

        Args:
            ctx (dict): context of job, mem etc.
            name (str): unique name for the job
            sent (callable): callable object
            temps_dir (str): unique path for strong job temps

        """
        raise NotImplementedError()

    def wait(self, immediate=False):
        """ Wait for a job to finish.

        KwArgs:
            immediate (bool): do not wait if no job has finished

        Returns:
            str: job name

        """
        raise NotImplementedError()

    def receive(self, name):
        """ Receive finished job.

        Args:
            name (str): name of job to retrieve

        Returns:
            object: received object

        """
        raise NotImplementedError()

    @property
    def length(self):
        """ Number of jobs in the queue. """
        raise NotImplementedError()

    @property
    def empty(self):
        """ Queue is empty. """
        raise NotImplementedError()


class SubmitError(Exception):
    pass


class ReceiveError(Exception):
    pass


