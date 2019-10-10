import os
import networkx
import itertools
import logging
import collections
import fnmatch
import logging

import pypeliner.helpers
import pypeliner.identifiers
import pypeliner.workflow


class IncompleteJobException(Exception):
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


class DependencyGraph:
    """ Graph of dependencies between jobs.
    """

    def __init__(self):
        self.completed = set()
        self.created = set()
        self.running = set()
        self.obsolete = set()

    def regenerate(self, jobs):
        """ Create the dependency graph from a set of jobs, and pipeline inputs
        and outputs, maintaining current state.

        """
        self.jobs = jobs
        all_inputs = set((input.id for job in self.jobs.values() for input in job.inputs))
        all_outputs = set((output.id for job in self.jobs.values() for output in job.outputs))
        self.inputs = all_inputs.difference(all_outputs)
        self.outputs = all_outputs.difference(all_inputs)

        self.dependant_jobs = collections.defaultdict(set)
        self.creating_job = dict()
        for job in jobs.values():
            for resource in job.inputs:
                self.dependant_jobs[resource.id].add(job.id)
            for resource in job.outputs:
                if resource.id in self.creating_job:
                    raise AmbiguousOutputException(resource.id, [job.id, self.creating_job[resource.id]])
                self.creating_job[resource.id] = job.id
        for job in jobs.values():
            for resource in job.inputs:
                if resource.id not in self.creating_job and resource.is_temp:
                    raise AmbiguousInputException(resource.id)

        # Create the graph
        G = networkx.DiGraph()
        for job in self.jobs.values():
            # only track job names, ignore the axes value.
            # this will keep DAG smaller and speed up cycle testing
            job_node = ('job', job.jobname)
            if G.has_node(job_node):
                continue
            G.add_node(job_node, job=job)
            chunks = [v.chunk for v in job.node]
            for input in job.inputs:
                input_chunks = [v.chunk for v in input.node]
                if not input_chunks == chunks:
                    continue
                resource_node = ('resource', input.id)
                if resource_node not in G:
                    G.add_node(resource_node, resource=input)
                G.add_edge(resource_node, job_node)
            for output in job.outputs:
                output_chunks = [v.chunk for v in output.node]
                if not output_chunks == chunks:
                    continue
                resource_node = ('resource', output.id)
                if resource_node not in G:
                    G.add_node(resource_node, resource=output)
                G.add_edge(job_node, resource_node)

        # Check for cycles
        try:
            cycles = networkx.find_cycle(G)
        except networkx.NetworkXNoCycle:
            cycles = []
        if len(cycles) > 0:
            raise DependencyCycleException(cycles[0])

        # Pre-compute traversals of the DAG
        self.jobs_forward = list(self.traverse_jobs_forward())
        self.jobs_reverse = list(self.traverse_jobs_reverse())

        # Assume pipeline inputs exist
        self.created.update(self.inputs)

    def traverse_jobs_forward(self):
        """ Traverse jobs in order of execution.
        """
        created_resources = set(self.inputs)

        adjacent_jobs = set()

        for job in self.jobs.values():
            if len(list(job.inputs)) == 0:
                adjacent_jobs.add(job.id)

        for resource_id in created_resources:
            for job_id in self.dependant_jobs[resource_id]:
                adjacent_jobs.add(job_id)

        while len(adjacent_jobs) > 0:
            for job_id in list(adjacent_jobs):
                job = self.jobs[job_id]
                if all([i.id in created_resources for i in job.inputs]):
                    yield job
                    adjacent_jobs.remove(job_id)
                    for o in job.outputs:
                        created_resources.add(o.id)
                        for dependent_job_id in self.dependant_jobs[o.id]:
                            adjacent_jobs.add(dependent_job_id)
                    break

    def traverse_jobs_reverse(self):
        """ Traverse jobs in order of execution.
        """
        visited_resources = set(self.outputs)
        visited_jobs = set()

        adjacent_resources = set()
        adjacent_jobs = set()

        for job in self.jobs.values():
            if len(list(job.outputs)) == 0:
                adjacent_jobs.add(job.id)

        for resource_id in visited_resources:
            if resource_id in self.creating_job:
                adjacent_jobs.add(self.creating_job[resource_id])

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
                if all([job_id in visited_jobs for job_id in self.dependant_jobs[resource_id]]):
                    adjacent_resources.remove(resource_id)
                    visited_resources.add(resource_id)
                    if resource_id in self.creating_job:
                        adjacent_jobs.add(self.creating_job[resource_id])

    def pop_next_job(self):
        """ Return the id of the next job that is ready for execution.
        """
        for job in self.jobs.values():
            if len(job.inputs) == 0 and job.id not in self.running and job.id not in self.completed:
                self.running.add(job.id)
                return job

        for job_id in self.running:
            if len(self.jobs[job_id].inputs) == 0:
                raise NoJobs()

        resource_out_of_date = set()

        resource_required = set()
        job_required = set()

        for job in self.jobs_forward:
            if job.id in self.completed:
                continue
            inputs_out_of_date = any([i.id in resource_out_of_date for i in job.inputs])
            if job.out_of_date() or inputs_out_of_date:
                for o in job.outputs:
                    resource_out_of_date.add(o.id)
                for i in job.inputs:
                    resource_required.add(i.id)

        for job in self.jobs_reverse:
            if job.id in self.completed:
                continue
            for o in job.outputs:
                if o.id in resource_out_of_date and not o.exists:
                    job_required.add(job.id)
                if o.id in resource_required and not o.exists:
                    job_required.add(job.id)
            if job.id in job_required:
                for i in job.inputs:
                    resource_required.add(i.id)

        for job in self.jobs_forward:
            inputs_created = all([i.id in self.created for i in job.inputs])
            if inputs_created and job.id not in self.running and job.id not in self.completed:
                if job.id in job_required:
                    job.is_required_downstream = True
                self.running.add(job.id)
                return self.jobs[job.id]

        raise NoJobs()

    def notify_completed(self, job_id):
        """ A job was completed, advance current state.
        """
        job = self.jobs[job_id]
        self.running.remove(job.id)
        self.completed.add(job.id)
        for input in job.inputs:
            if all([otherjob_id in self.completed for otherjob_id in self.dependant_jobs[input.id]]):
                self.obsolete.add(input)
        for output in job.outputs:
            if len(self.dependant_jobs[output.id]) == 0:
                self.obsolete.add(output)
        self.created.update([output.id for output in job.outputs])

    @property
    def finished(self):
        return all([output in self.created for output in self.outputs])

    def cleanup_obsolete(self):
        for resource in self.obsolete:
            resource.cleanup()
        self.obsolete = set()


class WorkflowInstance(object):
    def __init__(self, workflow_def, db_factory, runskip, node=pypeliner.identifiers.Node(), ctx={}, cleanup=False):
        self._logger = logging.getLogger('pypeliner.workflowgraph')
        self.workflow_def = workflow_def
        self.db_factory = db_factory
        self.runskip = runskip
        self.db = db_factory.create(workflow_def.path_info, node.subdir)
        self.node = node
        self.graph = DependencyGraph()
        self.subworkflows = list()
        self.cleanup = cleanup
        self.regenerate()
        self.ctx = workflow_def.ctx
        if ctx:
            self.ctx.update(ctx)

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
                job, workflow = pypeliner.helpers.pop_if(self.subworkflows, lambda j, w: w.finished)
            except IndexError:
                return

            # Complete the workflow
            self.complete_job(job)

    def add_subworkflow(self, job, workflow_def):
        node = self.node + job.node + pypeliner.identifiers.Namespace(job.job_def.name)
        workflow = WorkflowInstance(workflow_def, self.db_factory, self.runskip, node=node, cleanup=self.cleanup)
        self.subworkflows.append((job, workflow))

    def complete_job(self, job):
        self.db.job_shelf[job.displayname] = True
        self.notify_completed(job.id)

    def update_ctx(self, job):
        context_config = pypeliner.helpers.GlobalState.get("context_config")
        runskip = None
        job_displayname = job.displayname
        job_ctx = job.ctx
        if not context_config:
            return job

        contexts = context_config.get('context', {})
        for _,inpctx in contexts.items():
            if fnmatch.fnmatch(job_displayname, inpctx["name_match"]):
                job_ctx.update(inpctx.get("ctx", {}))
                runskip = inpctx.get("runskip")

        job.ctx = job_ctx
        job.runskip_request = runskip
        return job

    def pop_next_job(self):
        """ Pop the next job from the top of this or a subgraph.
        """

        while True:
            # Return any ready jobs from sub workflows
            for job, workflow in self.subworkflows:
                try:
                    return workflow.pop_next_job()
                except NoJobs:
                    pass

            # Finalize finished workflows
            self.finalize_workflows()

            # Remove from self graph if no subgraph jobs
            job = self.graph.pop_next_job()
            job = self.update_ctx(job)

            is_run_required, explaination = self.runskip(job)
            self._logger.info(
                'job ' + job.displayname + ' run: ' + str(is_run_required) + ' explanation: ' + explaination,
                extra={"id": job.displayname, "type": "job", "explanation": explaination, 'task_name': job.id[1]})
            if is_run_required:
                return job
            else:
                self.complete_job(job)
                self._logger.info(
                    'job ' + job.displayname + ' skipped',
                    extra={"id": job.displayname, "type": "job", "status": "skipped", 'task_name': job.id[1]})

    def notify_completed(self, job_id):
        self.graph.notify_completed(job_id)
        if self.cleanup:
            self.graph.cleanup_obsolete()

    @property
    def finished(self):
        return self.graph.finished
