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

    * if ``bwa samse`` fails and our script exits, ``bwa samse`` will be needlessly rerun despite
      valid results being available
    * ``tmp.sai`` is an intermediate file, and we dont really care where it resides, but it
      would be benefiical if it were cleaned up after the commands are run sucessfully
    * if input.fastq is too big, it would be beneficial to take advantage of the parallelizable
      nature of ``bwa``, and perform the process in smaller chunks, merging at the end

We will focus on the first 2 issues, then revisit the third in a subsequent section.
To run the same commands using pypeliner we first create a :py:class:`pypeliner.scheduler.Scheduler`
object::

    import pypeliner

    scheduler = pypeliner.scheduler.Scheduler()

Add the jobs to the scheduler by calling :py:func:`pypeliner.scheduler.Scheduler.commandline` (ignoring
the first 3 arguments for now)::

    scheduler.commandline('bwa_aln', (), {},
        'bwa', 'aln',
        pypeliner.managed.InputFile('genome.fasta'),
        pypeliner.managed.InputFile('input.fastq'),
        '>',
        pypeliner.managed.TempOutputFile('tmp.sai'))

    scheduler.commandline('bwa_samse', (), {},
        'bwa', 'samse',
        pypeliner.managed.InputFile('genome.fasta'),
        pypeliner.managed.TempInputFile('tmp.sai'),
        pypeliner.managed.InputFile('input.fastq'),
        '>',
        pypeliner.managed.TempOutputFile('output.sam'))

Then run the jobs using an execution queue::

    with pypeliner.execqueue.LocalJobQueue([]) as exec_queue: 
        scheduler.run(exec_queue)

Trivially, the syntax is slightly different for executing a command line.  The ``>`` argument will be familiar to `bash` users as a way of redirecting output to a file, and the meaning in pypeline is the same.

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

We can create a script using the above code, however, :py:class:`pypeliner.scheduler.Scheduler` objects have a number of
configuration options that we may like to set using a configuration file, and override with command line arguments to
our script.  Additionally, we might like to log the output of our ``bwa`` job.

To ease this process, pypeliner provides the :py:mod:`pypeliner.app` module.  We reimplement our pedagogic ``bwa``
example below.

Import pypeliner::

    import pypeliner

Create an ``argparse.ArgumentParser`` object to handle command line arguments
including a config, then parse the arguments::

    argparser = argparse.ArgumentParser()
    pypeliner.app.add_arguments(argparser)
    argparser.add_argument('config', help='Configuration Filename')
    argparser.add_argument('genome', help='Genome Fasta')
    argparser.add_argument('reads', help='Reads Fastq')
    argparser.add_argument('alignments', help='Alignments Sam')
    args = vars(argparser.parse_args())

Read in the config.  Here we are using a python syntax style config::

    config = {}
    execfile(args['config'], config)

Override config options with command line arguments::

    config.update(args)

Create a :py:class:`pypeliner.app.Pypeline` object with the config::

    pyp = pypeliner.app.Pypeline([], config)

Add the job and run::

    pyp.sch.commandline('bwa_aln', (), {},
        'bwa', 'aln',
        pypeliner.managed.InputFile(args['genome']),
        pypeliner.managed.InputFile(args['reads']),
        '>',
        pypeliner.managed.TempOutputFile('tmp.sai'))

    pyp.sch.commandline('bwa_samse', (), {},
        'bwa', 'samse',
        pypeliner.managed.InputFile(args['genome']),
        pypeliner.managed.TempInputFile('tmp.sai'),
        pypeliner.managed.InputFile(args['reads']),
        '>',
        pypeliner.managed.TempOutputFile(args['alignments']))

    pyp.sch.run()

Running the script above with ``-h`` shows the command line options available to control a pipeline.  The options are
described in the :py:mod:`pypeliner.app` api reference.

