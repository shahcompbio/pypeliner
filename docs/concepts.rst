.. _concepts:

Key Concepts
============

Overview
--------

A workflow in pypeliner is a set of jobs and subworkflows.  Jobs are either python functions or executables, together with a description of the arguments to those functions or executables.  A subworkflow is simply another workflow, an instance of which has been recursively added to a larger workflow.  Arguments in pypeliner can either be arbitrary python object that can be saved with pickle, or managed objects.  Managed objects are used to track dependencies between jobs, and themselves become additional arguments when a jobs function or executable is executed.

Workflows
---------

A workflow is a set of jobs and subworkflows.  The workflow specifies the set of named jobs that will be used to process data inputs and create data outputs.

Managed Files and Objects
-------------------------

Managed files and objects are named handles for files or objects managed by pypeliner.  Authoring a workflow involves defining the set of jobs and defining the common files or objects that those jobs work with as inputs and outputs.  Managed files and objects allow the workflow author to describe which arguments to a job are input files, output files, input objects or output objects, with some additional possibilities available.

Internally, managed objects will be turned into actual arguments to be passed to a command line or as parameters of a function.  This process is called _resolving_ an argument, and occurs before the job has been run, turning the managed object handle into an actual argument that can be passed to the command line or function.  After the job has finished, each argument is _finalized_, allowing for post-job steps specific to each argument.

Managed files are inputs and output files to jobs.  When a job is scheduled to be run, the pypeliner system will check the timestamp of input files against the output files.  A job for which all outputs are older than any input is skipped (unless overriden by options to the scheduler).  Output files are automatically resolved into a filename with '.tmp' appended, and during finalizing, the '.tmp' file is moved to the correct location, preventing the accidental use of partial output from a failed job.

Managed objects are input and output python objects.  As with managed files, the pypeliner system will check whether an input object is _newer_ than an output object.  This mechanism for this process is to keep a pickle file of the object value, and check the timestamp of that pickle file.  The pickle file is only updated if the value has changed.  In this way, both files and objects can be checked for their up to date / out of date status using timestamps.

Axes
----

The axes of a job in pypeliner refers to specific parameter sets over which the job is parallelized.  As an example, a job could be defined with axes 'patient_id', 'sample_id' for a job that runs per sample per patient.  Managed files and objects are similarly defined by the axes over which they are defined.  For instance, the dataset with name 'sample' may be defined with axes 'distribution_name', 'random_seed' if an object managed by pypeliner has a value per random seed per statistical distribution.

The axes of a managed object or file can be the same or different from the job that uses it.  If the job and managed file or object have the same axes, for each instance of the job, the matching instance of the associated file or object will be provided as an argument.

Chunks
------

Axes define how a job or dependency is parameterized.  Chunks are the actual values of those parameters.  For a chromosome axis, the chunks of that axis are the actual chromosome names.

Splits
------

An output defined on more axes than its corresponding job is a split output.  For instance, we may wish to output a set of results per chromosome, while working on the whole genome.  In this case, the output would have the additional chromosome axis.   For a managed output file with additional axes, the resolved argument is a dict like object that returns a filename on item access.  The keys expected by the dict like object are the chunks of the split axis or axes, expected as a value, or tuple of values for single or multiple split axes respectively.  In our examples, the dict like object would expect chromosome names as keys, and would provide per chromosome filenames.  A managed output object with additional axes can only be the return value of a transform job.  The job function is expected to return a dictionary of values, with keys as either single values or tuples depending on whether the managed object has single or multiple additional axes compared to the job.

Merges
------

An input defined on more axes than its corresponding job is a merge input.  For instance, we may have a set of results per chromosome and would like to unify these results into a single file using a single job.  In this case, the input would have an additional chromosome axis.  For both managed input objects managed input files, the resolved argument is a dictionary of objects or files with keys as the values, or tuples of values corresponding to the chunks of the merge axis or axes.

Subworkflows
------------

A subworkflow is a smaller workflow that has been encorporated as a module in a larger workflow.  Subworkflows are added to a workflow similarly to transform or command line jobs.  Additionally, pypeliner uses identical mechanisms to jobs for resolving arguments, and determining whether a subworkflow is out of date.  When determined to be out of date, the subworkflow function is executed and a workflow is expected as the return value.  For this reason, the return value of a subworkflow job cannot be a managed object object as it can for a regular transform job.  The subworkflows jobs are then recursively added to the scheduling system.




