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

    def __init__(self):
        self.completed = set()
        self.created = set()
        self.standby_jobs = unique_list()
        self.standby_resources = set()
        self.ready = set()
        self.running = set()
        self.obsolete = set()

    def regenerate(self, jobs):
        """ Create the dependency graph from a set of jobs, and pipeline inputs
        and outputs, maintaining current state.

        """

        self.jobs = jobs
        all_inputs = set((input.id for job in self.jobs.itervalues() for input in job.inputs))
        all_outputs = set((output.id for job in self.jobs.itervalues() for output in job.outputs))
        self.resource_ids = set(all_inputs)
        self.resource_ids.update(all_outputs)
        self.inputs = all_inputs.difference(all_outputs)
        self.outputs = all_outputs.difference(all_inputs)

        # Create the graph
        self.G = networkx.DiGraph()
        for job in self.jobs.itervalues():
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
                if node[1] not in self.inputs and len(created_by) == 0:
                    raise AmbiguousInputException(node[1])

        # Assume pipeline inputs exist
        self.created.update(self.inputs)
        self._notify_created(self.inputs)

        # Add all generative jobs as ready
        for job in self.jobs.itervalues():
            if len(list(job.inputs)) == 0:
                if job.id not in self.running and job.id not in self.completed:
                    self.ready.add(job.id)

    def traverse_jobs_forward(self):
        """ Traverse jobs in order of execution.
        """
        created_resources = set(self.inputs)

        adjacent_jobs = set()

        for job in self.jobs.itervalues():
            if len(list(job.inputs)) == 0:
                adjacent_jobs.add(job.id)

        for resource_id in created_resources:
            for job in self.get_dependant_jobs(resource_id):
                adjacent_jobs.add(job.id)
    
        while len(adjacent_jobs) > 0:
            for job_id in list(adjacent_jobs):
                job = self.jobs[job_id]
                if all([i.id in created_resources for i in job.inputs]):
                    yield job
                    adjacent_jobs.remove(job_id)
                    for o in job.outputs:
                        created_resources.add(o.id)
                        for dependent_job in self.get_dependant_jobs(o.id):
                            adjacent_jobs.add(dependent_job.id)
                    break

    def traverse_jobs_reverse(self):
        """ Traverse jobs in order of execution.
        """
        visited_resources = set(self.outputs)
        visited_jobs = set()

        adjacent_resources = set()
        adjacent_jobs = set()

        for job in self.jobs.itervalues():
            if len(list(job.outputs)) == 0:
                adjacent_jobs.add(job.id)

        for resource_id in visited_resources:
            creating_job = self.get_creating_job(resource_id)
            if creating_job is not None:
                adjacent_jobs.add(creating_job.id)

        while len(adjacent_jobs) > 0 or len(adjacent_resources) > 0:
            for job_id in list(adjacent_jobs):
                job = self.jobs[job_id]
                if all([o.id in visited_resources for o in job.outputs]):
                    yield job
                    adjacent_jobs.remove(job_id)
                    visited_jobs.add(job_id)
                    for i in job.inputs:
                        adjacent_resources.add(i.id)
                    break

            for resource_id in list(adjacent_resources):
                if all([j.id in visited_jobs for j in self.get_dependant_jobs(resource_id)]):
                    adjacent_resources.remove(resource_id)
                    visited_resources.add(resource_id)
                    creating_job = self.get_creating_job(resource_id)
                    if creating_job is not None:
                        adjacent_jobs.add(creating_job.id)

    def pop_next_job(self):
        """ Return the id of the next job that is ready for execution.
        """
        a = 0
        
        out_of_date = set()
        resource_required = set()
        job_required = set()

        for job in self.traverse_jobs_forward():
            inputs_out_of_date = any([i.id in out_of_date for i in job.inputs])
            if job.out_of_date or inputs_out_of_date:
                for o in job.outputs:
                    out_of_date.add(o.id)

        for job in self.traverse_jobs_reverse():
            for o in job.outputs:
                if o.id in out_of_date and not o.exists:
                    job_required.add(job.id)
                if o.id in resource_required and not o.exists:
                    job_required.add(job.id)
            if job.id in job_required:
                for i in job.inputs:
                    resource_required.add(i.id)

        for job in self.traverse_jobs_forward():
            inputs_created = all([i.id in self.created for i in job.inputs])
            if inputs_created and job.id not in self.running and job.id not in self.completed:
                if job.id in job_required:
                    job.is_required_downstream = True
                job_node = ('job', job.id)
                self.running.add(job.id)
                return self.G.node[job_node]['job']

        raise NoJobs()

    def get_resource(self, resource_id):
        """ Get resource object given resource id
        """
        return self.G.node[('resource', resource_id)]['resource']

    def get_dependant_jobs(self, resource_id):
        """ Generator for dependent jobs of a resource.
        """
        if not self.has_dependant_jobs(resource_id):
            return
        for _, job_id in self.G[('resource', resource_id)]:
            yield self.jobs[job_id]

    def has_dependant_jobs(self, resource_id):
        """ Check whether jobs depend on given resource.
        """
        return ('resource', resource_id) in self.G

    def get_creating_job(self, resource_id):
        """ Job creating a given resource.
        """
        job_ids = list([job_id for _, job_id in self.G.predecessors_iter(('resource', resource_id))])
        if len(job_ids) == 0:
            return None
        elif len(job_ids) == 1:
            return self.jobs[job_ids[0]]
        else:
            raise Exception('More than one creating job for ' + resource_id)

    def has_creating_job(self, resource_id):
        """ Check whether given resource has a creating job.
        """
        job_ids = list([job_id for _, job_id in self.G.predecessors_iter(('resource', resource_id))])
        return len(job_ids) > 0

    def notify_completed(self, job):
        """ A job was completed, advance current state.
        """
        self.running.remove(job.id)
        self.completed.add(job.id)
        for input in job.inputs:
            if all([otherjob.id in self.completed for otherjob in self.get_dependant_jobs(input.id)]):
                self.obsolete.add(input)
        for output in job.outputs:
            if not self.has_dependant_jobs(output.id):
                self.obsolete.add(output)
        self._notify_created([output.id for output in job.outputs])

    def _notify_created(self, outputs):
        """ A resource was created, update current state.
        """
        self.created.update(outputs)
        for output in outputs:
            for job in self.get_dependant_jobs(output):
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
            resource = self.get_resource(resource_id)
            if resource.exists:
                continue
            job = self.get_creating_job(resource_id)
            if job is None:
                continue
            job.is_required_downstream = True
            for input in job.inputs:
                if input.id not in required_resources:
                    required_resources.add(input.id)
                    visit_resources.add(input.id)

    @property
    def finished(self):
        return all([output in self.created for output in self.outputs])
        
    def cleanup_obsolete(self):
        for resource in self.obsolete:
            resource.cleanup()
        self.obsolete = set()


class WorkflowInstance(object):
    def __init__(self, workflow_def, db_factory, runskip, node=pypeliner.identifiers.Node(), cleanup=False):
        self._logger = logging.getLogger('workflowgraph')
        self.workflow_def = workflow_def
        self.db_factory = db_factory
        self.runskip = runskip
        self.db = db_factory.create(node.subdir)
        self.node = node
        self.graph = DependencyGraph()
        self.subworkflows = list()
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

        self.graph.regenerate(jobs)

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
                self._logger.info('subworkflow ' + job.displayname + ' explanation: ' + job.explain())
                if self.runskip(job):
                    job.check_inputs()
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
                    if workflow_def.empty:
                        self._logger.warning('subworkflow ' + job.displayname + ' returned an empty workflow\n' + received.log_text())
                    node = self.node + job.node + pypeliner.identifiers.Namespace(job.job_def.name)
                    workflow = WorkflowInstance(workflow_def, self.db_factory, self.runskip, node=node, cleanup=self.cleanup)
                    self.subworkflows.append((job, received, workflow))
                else:
                    self._logger.info('subworkflow ' + job.displayname + ' skipped')
                    job.complete()
                continue
            elif isinstance(job, pypeliner.jobs.SetObjInstance):
                self._logger.info('setting object ' + job.obj_displayname)
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
            self.graph.cleanup_obsolete()

    @property
    def finished(self):
        return self.graph.finished


