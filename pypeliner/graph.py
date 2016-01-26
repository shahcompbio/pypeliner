import os
import networkx
import itertools
import logging

import helpers
import identifiers
import database
import pypeliner.workflow

class IncompleteWorkflowException(Exception):
    pass

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
    def __init__(self, workflow_def, db_factory, node=identifiers.Node(), prune=False, cleanup=False, rerun=False, repopulate=False):
        self._logger = logging.getLogger('workflowgraph')
        self.workflow_def = workflow_def
        self.db_factory = db_factory
        self.db = db_factory.create(node.subdir)
        self.node = node
        self.graph = DependencyGraph()
        self.subworkflows = list()
        self.prune = prune
        self.cleanup = cleanup
        self.rerun = rerun
        self.repopulate = repopulate
        self.regenerate()

    def regenerate(self):
        """ Regenerate dependency graph based on job instances.
        """

        jobs = dict()
        for job_inst in self.workflow_def._create_job_instances(self, self.db):
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

    def finalize_workflows(self):
        """ Finalize any workflows that are finished.
        """

        while len(self.subworkflows) > 0:

            # Pop the next finished workflow if it exists
            try:
                job, received, workflow = helpers.pop_if(self.subworkflows, lambda (j, r, w): w.finished)
            except IndexError:
                return

            # Finalize the workflow
            job.finalize(received)
            job.complete()

    def pop_next_job(self):
        """ Pop the next job from the top of this or a subgraph.
        """
        
        while True:
            # Return any ready jobs from sub workflows
            for job, received, workflow in self.subworkflows:
                try:
                    return workflow.pop_next_job()
                except NoJobs:
                    pass

            # Finalize finished workflows
            self.finalize_workflows()

            # Remove from self graph if no subgraph jobs
            job = self.graph.pop_next_job()

            # Change axis jobs, to be depracated
            if job.is_immediate:
                job.finalize()
                job.complete()
                continue

            if job.is_subworkflow:
                if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                    send = job.create_callable()
                    self._logger.info('creating subworkflow ' + job.displayname)
                    self._logger.info('subworkflow ' + job.displayname + ' -> ' + send.displaycommand)
                    send()
                    received = send
                    if not received.finished:
                        self._logger.error('subworkflow ' + job.displayname + ' failed to complete\n' + received.log_text())
                        raise IncompleteWorkflowException()
                    workflow_def = received.ret_value
                    if not isinstance(workflow_def, pypeliner.workflow.Workflow):
                        self._logger.error('subworkflow ' + job.displayname + ' did not return a workflow\n' + received.log_text())
                        raise IncompleteWorkflowException()
                    node = self.node + job.node + identifiers.Namespace(job.job_def.name)
                    workflow = WorkflowInstance(workflow_def, self.db_factory, node=node, prune=self.prune, cleanup=self.cleanup, rerun=self.rerun, repopulate=self.repopulate)
                    self.subworkflows.append((job, received, workflow))
                else:
                    self._logger.info('subworkflow ' + job.displayname + ' skipped')
                    job.complete()
                self._logger.debug('subworkflow ' + job.displayname + ' explanation: ' + job.explain())
                continue
            elif job.ctx.get('immediate', False):
                if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                    send = job.create_callable()
                    self._logger.info('creating job ' + job.displayname)
                    self._logger.info('job ' + job.displayname + ' -> ' + send.displaycommand)
                    send()
                    received = send
                    if not received.finished:
                        self._logger.error('job ' + job.displayname + ' failed to complete\n' + received.log_text())
                        raise IncompleteWorkflowException()
                    job.finalize(received)
                    job.complete()
                else:
                    self._logger.info('job ' + job.displayname + ' skipped')
                    job.complete()
            else:
                return job

    def notify_completed(self, job):
        self.graph.notify_completed(job)
        if self.cleanup:
            self.db.resmgr.cleanup(self.graph)

    @property
    def finished(self):
        return self.graph.finished


