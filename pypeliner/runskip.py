

class BasicRunSkip(object):
    def __init__(self, repopulate=False, rerun=False):
        self.repopulate = repopulate
        self.rerun = rerun

    def __call__(self, job):
        return job.out_of_date or self.rerun or self.repopulate and job.output_missing
