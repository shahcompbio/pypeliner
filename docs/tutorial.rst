.. _tutorial:

Tutorial
========

Simple Example
--------------

Say we want to run the command line tool `bwa`, which involves 2 steps.  We can do so using
``subprocess``::

    import subprocess

    with open('tmp.sai', 'w') as f:
        subprocess.check_call(['bwa', 'aln',
                               'genome.fasta',
                               'input.fastq'], stdout=f)

    with open('output.sam', 'w') as f:
        subprocess.check_call(['bwa', 'samse',
                               'genome.fasta',
                               'tmp.sai',
                               'input.fastq'], stdout=f)

There are a number of issues with the above code that seem minor in this context but become
arduous when the size and complexity of the pipeline grows.

    * if ``bwa samse`` fails and our script exits, ``bwa aln`` will be needlessly rerun despite
      valid results being available
    * ``tmp.sai`` is an intermediate file, and we dont really care where it resides, but it
      would be benefiical if it were cleaned up after the commands are run sucessfully
    * if input.fastq is too big, it would be beneficial to take advantage of the parallelizable
      nature of ``bwa``, and perform the process in smaller chunks, merging at the end

We will focus on the first 2 issues, then revisit the third in a subsequent section.
To run the same commands using pypeliner we first create a :py:class:`pypeliner.workflow.Workflow`
object::

    import pypeliner.workflow

    workflow = pypeliner.workflow.Workflow()

Add the jobs to the workflow by calling :py:func:`pypeliner.workflow.Workflow.commandline`::

    workflow.commandline(
        name='bwa_aln',
        args=(
            'bwa', 'aln',
            pypeliner.managed.InputFile('genome.fasta'),
            pypeliner.managed.InputFile('input.fastq'),
            '>',
            pypeliner.managed.TempOutputFile('tmp.sai'),
        )
    )

    workflow.commandline(
        name='bwa_samse',
        args=(
            'bwa', 'samse',
            pypeliner.managed.InputFile('genome.fasta'),
            pypeliner.managed.TempInputFile('tmp.sai'),
            pypeliner.managed.InputFile('input.fastq'),
            '>',
            pypeliner.managed.TempOutputFile('output.sam'),
        )
    )

Then create a :py:class:`pypeliner.app.Pypeline` object and run the jobs::

    pyp = pypeliner.app.Pypeline()
    pyp.run(workflow)

Trivially, the syntax is slightly different for executing a command line.  The first argument gives a name to the
command line job.  We will ignore the second and third argument for now.  The ``>`` argument will be familiar to `bash`
users as a way of redirecting output to a file, and the meaning in pypeliner is the same.

Additionally, we are wrapping input and output files using the following classes:

 * :py:class:`pypeliner.managed.InputFile`
 * :py:class:`pypeliner.managed.TempInputFile`
 * :py:class:`pypeliner.managed.OutputFile`
 * :py:class:`pypeliner.managed.TempOutputFile`

By doing so, we are registring these filenames as inputs and outputs with the pypeliner system, with the benefit that
pypeliner can:

 * Create a dependency graph, run each command in the correct order, and detect cycles resulting from incorrectly
   defined pipelines.
 * Use modification times of files to determine which files are out of date, and thus which command line jobs need to
   be rerun.
 * Choose where to put ``tmp.sai``, releasing the developer from the burden of specifying full path information for
   temporary files.
 * Use the results stored in the ``tmp.sai`` temporary file if the ``bwa samse`` command line fails or is canceled
   and the script has to be rerun.
 * Remove ``tmp.sai`` when it is no longer required, for example when ``output.sam`` has been sucessfully created. 

Creating a pypeliner script
---------------------------

We can create a script using the above code that will run our ``bwa`` pipeline.  We can then run the ``bwa`` pipeline from the command line, providing filenames and additional pypeliner options as command line arguments.

Import pypeliner and argparse::

    import pypeliner
    import argparse

Create an ``argparse.ArgumentParser`` object to handle command line arguments
including a config, then parse the arguments::

    argparser = argparse.ArgumentParser()
    pypeliner.app.add_arguments(argparser)
    argparser.add_argument('genome', help='Genome Fasta')
    argparser.add_argument('reads', help='Reads Fastq')
    argparser.add_argument('alignments', help='Alignments Sam')
    args = vars(argparser.parse_args())

Create a :py:class:`pypeliner.app.Pypeline` object with the config as the command line arguments::

    pyp = pypeliner.app.Pypeline(config=args)

Create a :py:class:`pypeliner.workflow.Workflow` object to which we will add jobs::

    workflow = pypeliner.workflow.Workflow()

Add the job and run::

    genome_filename = args['genome']
    reads_filename = args['reads']
    alignments_filename = args['alignments']

    workflow.commandline(
        name='bwa_aln',
        args=(
            'bwa', 'aln',
            pypeliner.managed.InputFile(genome_filename),
            pypeliner.managed.InputFile(reads_filename),
            '>',
            pypeliner.managed.TempOutputFile('tmp.sai'),
        )
    )

    workflow.commandline(
        name='bwa_samse',
        args=(
            'bwa', 'samse',
            pypeliner.managed.InputFile(genome_filename),
            pypeliner.managed.TempInputFile('tmp.sai'),
            pypeliner.managed.InputFile(reads_filename),
            '>',
            pypeliner.managed.OutputFile(alignments_filename),
        )
    )

    pyp.sch.run(workflow)

Running the script above with ``-h`` shows the command line options available to control a pipeline.  The options are
described in the :py:mod:`pypeliner.app` api reference.

Adding a python function
------------------------

At some point in your pipeline you may wish to transform your data using a small amount of python code.  With pypeliner
you can add a python function job to your pipeline in a similar way to adding command line jobs.

For example, perhaps you have the following function that filters sam files (produced by ``bwa samse``) to remove
unmapped reads::

    def filter_unmapped(in_sam_filename, out_sam_filename):
        with open(in_sam_filename, 'r') as in_sam_file, open(out_sam_filename, 'w') as out_sam_file:
            for line in in_sam_file:
                fields = line.split('\t')
                if line.startswith('@') or not flag & 0x4:
                    out_sam_file.write(line)

We can add this function to the end of our pipeline and thus post-process the results of ``bwa samse`` using
:py:func:`pypeliner.scheduler.Scheduler.transform` as follows::

    workflow.commandline(
        name='bwa_samse',
        args=(
            'bwa', 'samse',
            pypeliner.managed.InputFile(genome_filename),
            pypeliner.managed.TempInputFile('tmp.sai'),
            pypeliner.managed.InputFile(reads_filename),
            '>',
            pypeliner.managed.TempOutputFile('raw.sam'),
        )
    )

    workflow.transform(
        name='filter_unmapped',
        func=filter_unmapped,
        args=(
            pypeliner.managed.TempInputFile('raw.sam'),
            pypeliner.managed.OutputFile(alignments_filename),
        )
    )

    pyp.sch.run(workflow)

Note that we have made the output of ``bwa samse`` a temporary output file named ``raw.sam``, and given it as input to
``filter_unmapped``.  The 4th argument to ``transform`` is the python function we wish to call.  The 5th can be
``None``, or a managed object that stores the return value of the function.  The remaining arguments, as ``*args`` and
``**kwargs`` are mapped directly to the ``*args`` and ``**kwargs`` of the specified function.

Note that the function will be packaged up and called remotely on a node if a cluster is being used, so there is no harm
in adding computationally intensive functionality to a transform.

The function must be importable, and thus cannot be defined in the main script.  A workaround if you want to define the
function in your main script is as follows::

    if __name__ == '__main__':

        import myscript

        ...

        workflow.transform('filter_unmapped', (), {},
            myscript.filter_unmapped,

        ...

    else:

        def filter_unmapped(in_sam_filename, out_sam_filename):

        ...

If the function is defined in a separate module, you must add it to the module list given as the first argument of
:py:class:`pypeliner.app.Pypeline`::

    import mymod

    pyp = pypeliner.app.Pypeline(modules=[mymod], config=config)

    ...

    workflow.transform('filter_unmapped', (), {},
        mymod.filter_unmapped,

    ...

Splitting and Merging
---------------------

If the input files to ``bwa aln`` and ``bwa samse`` are large, it may be beneficial to split these files into several
chunks and run ``bwa aln`` and ``bwa samse`` on each chunk independently.  To do so we need 2 ingredients: a python
function that splits the input file and a python function that merges the output file.

With pypeliner, we do not need to decide where to store the split files, that is done automatically.  To facilitate
this, we need to provide a split function that uses a callback function to determine the filename in which to store
each chunk::

    def split_file_byline(in_filename, lines_per_file, out_filename_callback):
        with open(in_filename, 'r') as in_file:
            def line_group(line, line_idx=itertools.count()):
                return int(next(line_idx) / lines_per_file)
            for file_idx, lines in itertools.groupby(in_file, key=line_group):
                with open(out_filename_callback(file_idx), 'w') as out_file:
                    for line in lines:
                        out_file.write(line)

For example, if our input file ``input.fastq`` has 2,500,000 reads (10,000,000 lines, 4 reads per line), calling
``split_file_byline`` function with arguments ``'input.fastq', 4*1000000, func`` will call ``func`` 3 times with
arguments ``0``, ``1`` and ``2``.

We can add this function to our pipeline as follows::

    workflow.transform(
        name='split_fastq',
        func=split_file_byline,
        args=(
            pypeliner.managed.InputFile('input.fastq'),
            4*1000000,
            pypeliner.managed.TempOutputFile('input.fastq', 'reads'),
        )
    )

Pypeliner will call ``split_file_byline``, providing a callback as the 3rd argument, and this callback will provide the
filename for each chunk of ``input.fastq``.  For our input file with 2,500,000 reads (10,000,000 lines), 3 files will be
created with chunk identifiers ``0``, ``1`` and ``2``.  This set of files are then refered to by the user as the temp
file ``'input.fastq', 'reads'`` (in contrast to the original input which has identifier ``'input.fastq'``).  Note that a
split output of a job must have the same set of axes as the job, and one additional axis, the split axis.

We can now use ``TempInputFile('input.fastq', 'reads')`` to refer to the files created by the split. The designator
``'reads'`` identifies the split axis.  However, we also need to specify that the ``bwa_aln`` and ``bwa_samse`` jobs
must be run once for each chunk of the ``'reads'`` split axis, inputting each chunk of ``'input.fastq', 'reads'``
and outputting ``'tmp.sai', 'reads'`` and then ``'raw.sam', 'reads'``.  To do so we simply add ``'reads'`` to the second
argument of each call to ``commandline`` or ``transform``, and to the appropriate managed objects::

    workflow.commandline(
        name='bwa_aln',
        axes=('reads',),
        args=(
            'bwa', 'aln',
            pypeliner.managed.InputFile(genome_filename),
            pypeliner.managed.TempInputFile('input.fastq', 'reads'),
            '>',
            pypeliner.managed.TempOutputFile('tmp.sai', 'reads'),
        )
    )

    workflow.commandline(
        name='bwa_samse',
        axes=('reads',),
        args=(
            'bwa', 'samse',
            pypeliner.managed.InputFile(genome_filename),
            pypeliner.managed.TempInputFile('tmp.sai', 'reads'),
            pypeliner.managed.TempInputFile('input.fastq', 'reads'),
            '>',
            pypeliner.managed.TempOutputFile('raw.sam', 'reads'),
        )
    )

Finally we require a merge function to create ``'raw.sam'`` from the set ``'raw.sam', 'reads'`` files.  A merge output of
a job must have the same set of axes as the job, and one additional axis, the merge axis.  For a merge output, pypeliner
provides a dictionary of the filenames (or objects) with chunk ids as keys.

Merging by line is sufficient for sam files::

    def merge_file_byline(in_filenames, out_filename):
        with open(out_filename, 'w') as out_file:
            for id, in_filename in sorted(in_filenames.items()):
                with open(in_filename, 'r') as in_file:
                    for line in in_file.readlines():
                        out_file.write(line)

We can add this merge job with an additional transform::

    workflow.transform(
        name='merge_sam',
        func=merge_file_byline,
        args=(
            pypeliner.managed.TempInputFile('raw.sam', 'reads'),
            pypeliner.managed.TempOutputFile('raw.sam'),
        )
    )

Creating a workflow function
----------------------------

Now that we have defined a set of jobs that accomplish a task, we would like to be able to resuse this functionality in other contexts.  One way would be to create a script that runs the above pipeline and call that pipeline from other pipelines.  Alternatively, we can create a workflow function that can be recursively included in other pypeliner pipelines.

A workflow function is simply a function that returns a :py:class:`pypeliner.workflow.Workflow` object.  For example, an abbreviated version of the bwa align workflow function would be::

    def create_bwa_align_workflow(genome_filename, reads_filename, alignments_filename):
        workflow = pypeliner.workflow.Workflow()

        workflow.transform(
            name='split_fastq'
            ...
        )

        workflow.commandline(
            name='bwa_aln',
            ...
        )

        workflow.commandline(
            name='bwa_samse',
            ...
        )

        workflow.transform(
            name='merge_sam',
            ...
        )

        return workflow

Workflow functions are useful for creating larger workflows composed of smaller sub-workflows.

Subworkflows
------------

A subworkflow is a smaller workflow that has been encorporated as a module in a larger workflow.  Subworkflows are added to a workflow similarly to transform or command line jobs.  We provide pypeliner with the function to execute to create the subworkflow, and a set of arguments to that function, some of which will be managed objects.  Pypeliner will then execute the function, and create and run the subworkflow, conditional on the inputs and outputs requiring a rerun.  

Continuing our example, perhaps we would like to extract the reads from a bam and do bwa alignment using the bwa alignment workflow as a subworkflow::

    bam_filename = 'sample.bam'
    alignments_filename = 'alignments.sam'

	workflow = pypeliner.workflow.Workflow()

    workflow.subworkflow(
    	name='extract_reads',
    	func=extract_reads,
    	args=(
    		managed.InputFile(bam_filename),
    		managed.TempOutputFile('reads.fastq'),
    	),
    )

    workflow.subworkflow(
        name='bwa_workflow',
        func=create_bwa_align_workflow,
        args=(
        	genome_filename,
            managed.TempInputFile('reads.fastq'),
        	managed.OutputFile(alignments_filename),
        ),
    )

The above workflow will first extract reads with the ``extract_reads`` function, then check the inputs and outputs of the ``bwa_workflow`` job.  If the outputs are out of date, pypeliner will run the ``create_bwa_align_workflow`` function with the 3 arguments specified, resolving the temp input and output appropriately.  The resulting workflow will be used to add more jobs to the scheduling system.  If the outputs are up to date, the subworkflow will be skipped.


