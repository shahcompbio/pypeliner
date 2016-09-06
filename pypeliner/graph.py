import os
import networkx
import itertools
import logging

import pypeliner.helpers
import pypeliner.identifiers
import pypeliner.workflow

class IncompleteJobException(Exception):
    pass

class IncompleteWorkflowException(Exception):
    pass

class AmbiguousInputException(Exception):
    def __init__(self, id):
        self.id = id
    def __str__(self):
        return 'temp input {0} not created by any job'.format(self.id)

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


class unique_list(object):
    def __init__(self):
        self.l = list()
        self.s = set()
    def __len__(self):
        return len(self.l)
    def append(self, v):
        if v not in self.s:
            self.l.append(v)
            self.s.add(v)
    def pop_front(self):
        v = self.l.pop(0)
        self.s.remove(v)
        return v


class DependencyGraph:
    """ Graph of dependencies between jobs.
    """

    def __init__(self, db):
        self.db = db
        self.completed = set()
        self.created = set()
        self.standby_jobs = unique_list()
        self.standby_resources = set()
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
            job_node = ('job', job.id)
            self.G.add_node(job_node, job=job)
            for input in job.inputs:
                resource_node = ('resource', input.id)
                if resource_node not in self.G:
                    self.G.add_node(resource_node, resource=input)
                self.G.add_edge(resource_node, job_node)
            for output in job.outputs:
                resource_node = ('resource', output.id)
                if resource_node not in self.G:
                    self.G.add_node(resource_node, resource=output)
                self.G.add_edge(job_node, resource_node)

        # Check for cycles
        try:
            cycles = networkx.find_cycle(self.G)
        except networkx.NetworkXNoCycle:
            cycles = []
        if len(cycles) > 0:
            raise DependencyCycleException(cycles[0])

        # Check for ambiguous output
        # and temps with no creating job
        for node in self.G.nodes():
            if node[0] == 'resource':
                created_by = [edge[0][1] for edge in self.G.in_edges(node)]
                if len(created_by) > 1:
                    raise AmbiguousOutputException(node[1], created_by)
                if node[1] not in inputs and len(created_by) == 0:
                    raise AmbiguousInputException(node[1])

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
        if len(self.ready) > 0:
            job_id = self.ready.pop()
        elif len(self.standby_jobs) > 0:
            job_id = self.standby_jobs.pop_front()
        else:
            raise NoJobs()

        job_node = ('job', job_id)

        assert job_id not in self.running
        assert job_id not in self.completed

        self.running.add(job_id)

        return self.G.node[job_node]['job']

    def notify_completed(self, job):
        """ A job was completed, advance current state.
        """
        self.running.remove(job.id)
        self.completed.add(job.id)
        for input in job.inputs:
            if all([otherjob_id in self.completed for _, otherjob_id in self.G[('resource', input.id)]]):
                self.obsolete.add(input.id)
        for output in job.outputs:
            if ('resource', output.id) not in self.G:
                self.obsolete.add(output.id)
        self._notify_created([output.id for output in job.outputs])

    def _notify_created(self, outputs):
        """ A resource was created, update current state.
        """
        self.created.update(outputs)
        for output in outputs:
            for job_node in self.G[('resource', output)]:
                job = self.G.node[job_node]['job']
                if job.id in self.running:
                    continue
                if job.id in self.completed:
                    continue
                job_inputs = set([input.id for input in job.inputs])
                not_created_inputs = job_inputs.difference(self.created)
                not_created_standby_inputs = not_created_inputs.intersection(self.standby_resources)
                if len(not_created_inputs) == 0:
                    if not job.out_of_date() and job.output_missing():
                        self.standby_jobs.append(job.id)
                        logging.getLogger('jobgraph').debug('job {} in standby'.format(job.id))
                        for output in job.outputs:
                            if not output.exists:
                                self.standby_resources.add(output.id)
                    else:
                        self.ready.add(job.id)
                elif len(not_created_standby_inputs) > 0:
                    logging.getLogger('jobgraph').debug('job {} notify required'.format(job.id) + str(not_created_standby_inputs))
                    self._notify_required(not_created_standby_inputs)

    def _notify_required(self, required):
        """

        Visit all jobs upstream of the out of date resources,
        mark the jobs outputting those resources as required
        by downstream jobs.

        For any inputs of the job that do not exist, mark
        those inputs as required and visit.
        """

        required_resources = set()
        visit_resources = set(required)
        while len(visit_resources) > 0:
            resource_id = visit_resources.pop()
            resource_node = ('resource', resource_id)
            resource = self.G.node[resource_node]['resource']
            if resource.exists:
                continue
            for job_node in self.G.predecessors_iter(resource_node):
                job = self.G.node[job_node]['job']
                job.is_required_downstream = True
                for input in job.inputs:
                    if input.id not in required_resources:
                        required_resources.add(input.id)
                        visit_resources.add(input.id)

    @property
    def finished(self):
        return all([output in self.created for output in self.outputs])


class WorkflowInstance(object):
    def __init__(self, workflow_def, db_factory, runskip, node=pypeliner.identifiers.Node(), prune=False, cleanup=False):
        self._logger = logging.getLogger('workflowgraph')
        self.workflow_def = workflow_def
        self.db_factory = db_factory
        self.runskip = runskip
        self.db = db_factory.create(node.subdir)
        self.node = node
        self.graph = DependencyGraph(self.db)
        self.subworkflows = list()
        self.prune = prune
        self.cleanup = cleanup
        self.regenerate()

    def regenerate(self):
        """ Regenerate dependency graph based on job instances.
        """

        jobs = dict()
        for job_inst in self.workflow_def._create_job_instances(self, self.db):
            if job_inst.id in jobs:
                raise ValueError('Duplicate job ' + job_inst.displayname)
            jobs[job_inst.id] = job_inst

        inputs = set((input.id for job in jobs.itervalues() for input in job.pipeline_inputs))
        if self.prune:
            outputs = set((output.id for job in jobs.itervalues() for output in job.pipeline_outputs))
        else:
            outputs = set((output.id for job in jobs.itervalues() for output in job.outputs))
        inputs = inputs.difference(outputs)

        self.graph.regenerate(inputs, outputs, jobs.values())

    def finalize_workflows(self):
        """ Finalize any workflows that are finished.
        """

        while len(self.subworkflows) > 0:

            # Pop the next finished workflow if it exists
            try:
                job, received, workflow = pypeliner.helpers.pop_if(self.subworkflows, lambda (j, r, w): w.finished)
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

            if isinstance(job, pypeliner.jobs.SubWorkflowInstance):
                self._logger.debug('subworkflow ' + job.displayname + ' explanation: ' + job.explain())
                if self.runskip(job):
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
                    node = self.node + job.node + pypeliner.identifiers.Namespace(job.job_def.name)
                    workflow = WorkflowInstance(workflow_def, self.db_factory, self.runskip, node=node, prune=self.prune, cleanup=self.cleanup)
                    self.subworkflows.append((job, received, workflow))
                else:
                    self._logger.info('subworkflow ' + job.displayname + ' skipped')
                    job.complete()
                continue
            elif isinstance(job, pypeliner.jobs.SetObjInstance):
                self._logger.debug('setting object ' + job.obj_displayname)
                send = job.create_callable()
                send()
                received = send
                if not received.finished:
                    self._logger.error('setting object ' + job.obj_displayname + ' failed to complete\n' + received.log_text())
                    raise IncompleteJobException()
                job.finalize(received)
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


