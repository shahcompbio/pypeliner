import os
import collections
import networkx
import itertools
import logging

import helpers
import nodes
import resourcemgr

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

class NoJobs(Exception):
    pass

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

    def pop_next_job(self):
        """ Return the id of the next job that is ready for execution.
        """
        if len(self.ready) == 0:
            raise NoJobs()

        job_id = self.ready.pop()
        job_node = ('job', job_id)

        self.running.add(job_id)

        return self.G.node[job_node]['job']

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
    def finished(self):
        return all([output in self.created for output in self.outputs])


class WorkflowInstance(object):
    def __init__(self, workflow_def, workflow_dir, dir_lock, node=nodes.Node(), prune=False, cleanup=False):
        self._logger = logging.getLogger('workflowgraph')
        self.workflow_def = workflow_def
        self.workflow_dir = workflow_dir
        self.dir_lock = dir_lock

        db_dir = os.path.join(workflow_dir, 'db', node.subdir)
        nodes_dir = os.path.join(workflow_dir, 'nodes', node.subdir)
        temps_dir = os.path.join(workflow_dir, 'tmp', node.subdir)
        self.logs_dir = os.path.join(workflow_dir, 'log', node.subdir)

        helpers.makedirs(db_dir)
        helpers.makedirs(nodes_dir)
        helpers.makedirs(temps_dir)
        helpers.makedirs(self.logs_dir)

        self.dir_lock.add_lock(os.path.join(db_dir, '_lock'))
        self.resmgr = resourcemgr.ResourceManager(temps_dir, db_dir)
        self.nodemgr = nodes.NodeManager(nodes_dir, temps_dir)
        self.node = node
        self.graph = DependencyGraph()
        self.subworkflows = list()
        self.prune = prune
        self.cleanup = cleanup
        self.regenerate()

    def regenerate(self):
        """ Regenerate dependency graph based on job instances.
        """

        jobs = dict()
        for job_inst in self.workflow_def._create_job_instances(self, self.resmgr, self.nodemgr, self.logs_dir):
            if job_inst.id in jobs:
                raise ValueError('Duplicate job ' + job_inst.displayname)
            jobs[job_inst.id] = job_inst

        inputs = set((input for job in jobs.itervalues() for input in job.pipeline_inputs))
        if self.prune:
            outputs = set((output for job in jobs.itervalues() for output in job.pipeline_outputs))
        else:
            outputs = set((output for job in jobs.itervalues() for output in job.outputs))
        inputs = inputs.difference(outputs)

        self.graph.regenerate(inputs, outputs, jobs.values())

    def pop_next_job(self):
        """ Pop the next job from the top of this or a subgraph.
        """
        
        while True:
            # Check if sub workflows have ready jobs, or have finished
            for job, graph in self.subworkflows:
                try:
                    return graph.pop_next_job()
                except NoJobs:
                    pass
                if graph.finished:
                    job.finalize()
                    job.complete()

            # Remove finished sub workflows
            self.subworkflows = filter(lambda (job, graph): not graph.finished, self.subworkflows)

            # Remove from self graph if no subgraph jobs
            job = self.graph.pop_next_job()

            if job.is_subworkflow:
                self._logger.info('creating subworkflow ' + job.displayname)
                workflow_def = job.create_subworkflow()
                node = self.node + job.node + nodes.Namespace(job.job_def.name)
                workflow = WorkflowInstance(workflow_def, self.workflow_dir, self.dir_lock, node=node, prune=self.prune, cleanup=self.cleanup)
                self.subworkflows.append((job, workflow))
                continue
            elif job.is_immediate:
                job.finalize()
                job.complete()
                continue
            else:
                return job

    def notify_completed(self, job):
        self.graph.notify_completed(job)
        if self.cleanup:
            self.resmgr.cleanup(self.graph)

    @property
    def finished(self):
        return self.graph.finished


