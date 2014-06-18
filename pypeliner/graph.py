import collections

class AmbiguousInputException(Exception):
    def __init__(self, id):
        self.id = id
    def __str__(self):
        return 'input {0} not created by any job'.format(self.id)

class AmbiguousOutputException(Exception):
    def __init__(self, output, job1, job2):
        self.output = output
        self.job1 = job1
        self.job2 = job2
    def __str__(self):
        return 'output {0} created by jobs {1} and {2}'.format(self.output, self.job1, self.job2)

class DependencyCycleException(Exception):
    def __init__(self, id):
        self.id = id
    def __str__(self):
        return 'dependency cycle involving output {0}'.format(self.id)

class DependencyGraph:
    """ Graph of dependencies between jobs """
    def __init__(self):
        self.completed = set()
        self.created = set()
        self.ready = set()
        self.running = set()
        self.obsolete = set()
    def regenerate(self, inputs, outputs, jobs):
        self.inputs = inputs
        self.outputs = outputs
        self.created.update(inputs)
        self.forward = collections.defaultdict(set)
        backward = dict()
        for job in jobs:
            for output in job.outputs:
                if output in backward:
                    raise AmbiguousOutputException(output, job.id, backward[output].id)
                backward[output] = job
        self.forward = collections.defaultdict(set)
        tovisit = list(self.outputs)
        paths = list(set() for a in self.outputs)
        visited = set()
        required = set()
        generative = set()
        while len(tovisit) > 0:
            visit = tovisit.pop()
            path = paths.pop()
            if visit in visited:
                continue
            visited.add(visit)
            if visit in self.created:
                required.add(visit)
                continue
            elif visit not in backward:
                raise AmbiguousInputException(visit)
            if len(list(backward[visit].inputs)) == 0:
                generative.add(backward[visit])
                continue
            for input in backward[visit].inputs:
                if backward[visit].id in path:
                    raise DependencyCycleException(visit)
                self.forward[input].add(backward[visit])
                tovisit.append(input)
                paths.append(path.union(set([backward[visit].id])))
        self._notify_created(required)
        for job in generative:
            if job.id not in self.running and job.id not in self.completed:
                self.ready.add(job.id)
    def next_job(self):
        job_id = self.ready.pop()
        self.running.add(job_id)
        return job_id
    def notify_completed(self, job):
        self.running.remove(job.id)
        self.completed.add(job.id)
        for input in job.inputs:
            if all([otherjob.id in self.completed for otherjob in self.forward[input]]):
                self.obsolete.add(input)
        for output in job.outputs:
            if output not in self.forward:
                self.obsolete.add(output)
        self._notify_created([output for output in job.outputs])
    def _notify_created(self, outputs):
        self.created.update(outputs)
        for output in outputs:
            if output not in self.forward:
                continue
            for job in self.forward[output]:
                if all([input in self.created for input in job.inputs]) and job.id not in self.running and job.id not in self.completed:
                    self.ready.add(job.id)
    @property
    def jobs_ready(self):
        return len(self.ready) > 0
    @property
    def finished(self):
        return all([output in self.created for output in self.outputs])
