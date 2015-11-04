import collections
import networkx
import itertools

class AmbiguousInputException(Exception):
    def __init__(self, id):
        self.id = id
    def __str__(self):
        return 'input {0} not created by any job'.format(self.id)

class AmbiguousOutputException(Exception):
    def __init__(self, output, jobs):
        self.output = output
        self.jobs = jobs
    def __str__(self):
        return 'output {0} created by jobs {1}'.format(self.output, ' and '.join([str(j) for j in self.jobs]))

class DependencyCycleException(Exception):
    def __init__(self, cycle):
        self.cycle = cycle
    def __str__(self):
        return 'dependency cycle {0}'.format(self.cycle)

class DependencyGraph:
    """ Graph of dependencies between jobs.
    """

    def __init__(self):
        self.completed = set()
        self.created = set()
        self.ready = set()
        self.running = set()
        self.obsolete = set()

    def regenerate(self, inputs, outputs, jobs):
        """ Create the dependency graph from a set of jobs, and pipeline inputs
        and outputs, maintaining current state.

        """

        self.outputs = outputs

        # Create the graph
        self.G = networkx.DiGraph()
        for job in jobs:
            self.G.add_node(('job', job.id), job=job)
        for resource in itertools.chain(inputs, outputs):
            self.G.add_node(('resource', resource), resource=resource)
        for job in jobs:
            for input in job.inputs:
                self.G.add_edge(('resource', input), ('job', job.id))
            for output in job.outputs:
                self.G.add_edge(('job', job.id), ('resource', output))

        # Check for cycles
        try:
            cycles = networkx.find_cycle(self.G)
        except networkx.NetworkXNoCycle:
            cycles = []
        if len(cycles) > 0:
            raise DependencyCycleException(cycles[0])

        # Check for ambiguous output
        for node in self.G.nodes():
            if node[0] == 'resource':
                created_by = [edge[0][1] for edge in self.G.in_edges(node)]
                if len(created_by) > 1:
                    raise AmbiguousOutputException(node[1], created_by)

        # Assume pipeline inputs exist
        self.created.update(inputs)
        self._notify_created(inputs)

        # Add all generative jobs as ready
        for job in jobs:
            if len(list(job.inputs)) == 0:
                if job.id not in self.running and job.id not in self.completed:
                    self.ready.add(job.id)

    def next_job(self):
        """ Return the id of the next job that is ready for execution.
        """
        job_id = self.ready.pop()
        self.running.add(job_id)
        return job_id

    def notify_completed(self, job):
        """ A job was completed, advance current state.
        """
        self.running.remove(job.id)
        self.completed.add(job.id)
        for input in job.inputs:
            if all([otherjob_id in self.completed for _, otherjob_id in self.G[('resource', input)]]):
                self.obsolete.add(input)
        for output in job.outputs:
            if ('resource', output) not in self.G:
                self.obsolete.add(output)
        self._notify_created([output for output in job.outputs])

    def _notify_created(self, outputs):
        """ A resource was created, update current state.
        """
        self.created.update(outputs)
        for output in outputs:
            for job_node in self.G[('resource', output)]:
                job = self.G.node[job_node]['job']
                if all([input in self.created for input in job.inputs]) and job.id not in self.running and job.id not in self.completed:
                    self.ready.add(job.id)

    @property
    def jobs_ready(self):
        return len(self.ready) > 0

    @property
    def finished(self):
        return all([output in self.created for output in self.outputs])

