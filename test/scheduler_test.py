import unittest
import shutil
import glob
import os
import time
import logging
import itertools

import pypeliner
import pypeliner.workflow
import pypeliner.managed as mgd

if __name__ == '__main__':

    import scheduler_test
    from scheduler_test import *

    script_directory = os.path.dirname(os.path.abspath(__file__))

    pipeline_dir = os.path.join(script_directory, 'pipeline')
    
    with pypeliner.execqueue.LocalJobQueue([scheduler_test]) as exec_queue:

        class scheduler_test(unittest.TestCase):

            input_filename = os.path.join(script_directory, 'scheduler_test.input')
            output_filename = os.path.join(script_directory, 'scheduler_test.output')

            input_n_filename = os.path.join(script_directory, 'scheduler_test.{byfile}.input')
            output_n_filename = os.path.join(script_directory, 'scheduler_test.{byfile}.output')
            output_n_template = os.path.join(script_directory, 'scheduler_test.{byfile}.template')

            input_1_filename = input_n_filename.format(byfile=1)
            input_2_filename = input_n_filename.format(byfile=2)

            output_1_filename = output_n_filename.format(byfile=1)
            output_2_filename = output_n_filename.format(byfile=2)

            log_filename = os.path.join(script_directory, './scheduler_test.log')

            try:
                os.remove(log_filename)
            except OSError:
                pass

            logging.basicConfig(level=logging.DEBUG, filename=log_filename, filemode='a')
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            console.setFormatter(pypeliner.helpers.MultiLineFormatter('%(asctime)s - %(name)s - %(levelname)s - '))
            logging.getLogger('').addHandler(console)

            ctx = dict({'mem':1})

            def setUp(self):

                try:
                    shutil.rmtree(pipeline_dir)
                except:
                    pass

                try:
                    shutil.rmtree(exc_prefix)
                except:
                    pass

                try:
                    os.remove(self.output_filename)
                except OSError:
                    pass

                for chunk in (1, 2):
                    try:
                        os.remove(self.output_n_filename.format(**{'byfile':chunk}))
                        os.remove(self.output_n_template.format(**{'byfile':chunk}))
                    except OSError:
                        pass

            def create_scheduler(self):

                scheduler = pypeliner.scheduler.Scheduler()
                scheduler.workflow_dir = pipeline_dir
                scheduler.max_jobs = 10
                return scheduler

            def test_simple_chunks1(self):

                workflow = pypeliner.workflow.Workflow()

                # Write a set of output files indexed by axis `byfile`
                workflow.transform('write_files', (), self.ctx, write_files,
                    None,
                    mgd.OutputFile(self.output_n_filename, 'byfile'))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                for chunk in ('1', '2'):
                    with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                        output = output_file.readlines()
                        self.assertEqual(output, ['file{0}\n'.format(chunk)])

            def test_simple_chunks2(self):

                workflow = pypeliner.workflow.Workflow()

                # Directly set the chunks indexed by axis `byfile`
                workflow.transform('set_chunks', (), self.ctx, set_chunks,
                    mgd.OutputChunks('byfile'))

                # Transform the input files indexed by axis `byfile` to output files
                # also indexed by axis `byfile`
                workflow.transform('do', ('byfile',), self.ctx, file_transform,
                    None,
                    mgd.InputFile(self.input_n_filename, 'byfile'),
                    mgd.OutputFile(self.output_n_filename, 'byfile'),
                    mgd.InputInstance('byfile'),
                    mgd.Template(self.output_n_template, 'byfile'))
                
                # Merge output files indexed by axis `byfile` into a single output file
                workflow.transform('merge', (), self.ctx, merge_file_byline,
                    None,
                    mgd.InputFile(self.output_n_filename, 'byfile'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                for chunk in ('1', '2'):
                    self.assertTrue(os.path.exists(self.output_n_template.format(**{'byfile':chunk})))
                    with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                        output = output_file.readlines()
                        self.assertEqual(output, [chunk + 'line' + str(line_num) + '\n' for line_num in range(1, 9)])

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()
                    self.assertEqual(output, [chunk + 'line' + str(line_num) + '\n' for chunk in ('1', '2') for line_num in range(1, 9)])

            def test_simple(self):

                workflow = pypeliner.workflow.Workflow()

                # Read data into a managed object
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                workflow.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                workflow.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

            def test_dict_args(self):

                workflow = pypeliner.workflow.Workflow()

                workflow.transform('dict_arg_stuff', (), self.ctx, dict_arg_stuff,
                    None,
                    {'1':mgd.OutputFile(self.output_1_filename),
                     '2':mgd.OutputFile(self.output_2_filename)},
                    {'1':mgd.InputFile(self.input_1_filename),
                     '2':mgd.InputFile(self.input_2_filename)})

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                for file_num in (1, 2):
                    with open(self.output_n_filename.format(byfile=file_num), 'r') as output_file:
                        output = output_file.read()
                    expected = 'line1{0}\nline2{0}\nline3{0}\nline4{0}\nline5{0}\nline6{0}\nline7{0}\nline8{0}\n'.format(file_num)
                    self.assertEqual(output, expected)

            def test_simple_sub_workflow(self):

                workflow = pypeliner.workflow.Workflow()

                workflow.setobj(mgd.OutputChunks('byfile'), (1, 2))

                workflow.transform('append_to_lines', ('byfile',), self.ctx,
                    append_to_lines,
                    None,
                    mgd.InputFile(self.input_n_filename, 'byfile'),
                    '#',
                    mgd.TempOutputFile('intermediate1', 'byfile'))

                workflow.subworkflow('sub_workflow_1', ('byfile',),
                    create_workflow_1,
                    mgd.TempInputFile('intermediate1', 'byfile'),
                    mgd.TempOutputFile('intermediate2', 'byfile'))

                workflow.transform('merge_files', (), self.ctx,
                    merge_file_byline,
                    None,
                    mgd.TempInputFile('intermediate2', 'byfile'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                expected = ['0b0aline1#\n', '1b1aline2#\n', '2b2aline3#\n', '3b3aline4#\n',
                    '4b4aline5#\n', '5b5aline6#\n', '6b6aline7#\n', '7b7aline8#-\n',
                    '0b0aline1#\n', '1b1aline2#\n', '2b2aline3#\n', '3b3aline4#\n',
                    '4b4aline5#\n', '5b5aline6#\n', '6b6aline7#\n', '7b7aline8#-\n']

                self.assertEqual(output, expected)

            def test_specify_input_filename(self):

                workflow = pypeliner.workflow.Workflow()

                # For single axis, tuple or value should work
                input_filenames = {
                    (1,):self.input_n_filename.format(byfile=1),
                    2:self.input_n_filename.format(byfile=2),
                }

                # Merge a set of input files indexed by axis `byfile`
                workflow.setobj(mgd.OutputChunks('byfile'), (1, 2))
                workflow.transform('merge_files', (), self.ctx, merge_file_byline,
                    None,
                    mgd.InputFile('input_files', 'byfile', fnames=input_filenames),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n', 'line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

            def test_specify_input_filename_template(self):

                workflow = pypeliner.workflow.Workflow()

                # Merge a set of input files indexed by axis `byfile`
                workflow.setobj(mgd.OutputChunks('byfile'), (1, 2))
                workflow.transform('merge_files', (), self.ctx, merge_file_byline,
                    None,
                    mgd.InputFile('input_files', 'byfile', template=self.input_n_filename),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n', 'line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

            def test_specify_output_filename(self):

                workflow = pypeliner.workflow.Workflow()

                # For single axis, tuple or value should work
                output_filenames = {
                    1:self.output_n_filename.format(byfile=1),
                    (2,):self.output_n_filename.format(byfile=2),
                }

                # Write a set of output files indexed by axis `byfile`
                workflow.transform('write_files', (), self.ctx, write_files,
                    None,
                    mgd.OutputFile('output_files', 'byfile', fnames=output_filenames))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                for chunk in ('1', '2'):
                    with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                        output = output_file.readlines()
                        self.assertEqual(output, ['file{0}\n'.format(chunk)])

            def test_specify_output_filename_template(self):

                workflow = pypeliner.workflow.Workflow()

                # Write a set of output files indexed by axis `byfile`
                workflow.transform('write_files', (), self.ctx, write_files,
                    None,
                    mgd.OutputFile('output_files', 'byfile', template=self.output_n_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                for chunk in ('1', '2'):
                    with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                        output = output_file.readlines()
                        self.assertEqual(output, ['file{0}\n'.format(chunk)])

            def test_set_multiple_axes(self):

                workflow = pypeliner.workflow.Workflow()

                # Merge a set of input files indexed by axis `byfile`
                workflow.setobj(mgd.OutputChunks('byfile', 'bychar'), ((1, 'a'), (1, 'b'), (2, 'a')))
                workflow.transform('merge_files', (), self.ctx, merge_file_byline,
                    None,
                    mgd.InputFile('input_files', 'byfile', template=self.input_n_filename),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n', 'line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

            def test_tempfile(self):

                workflow = pypeliner.workflow.Workflow()

                # Write the name of the temp file produced by pypeliner
                # into an output file
                workflow.transform('write_files', (), self.ctx, check_temp,
                    None,
                    mgd.OutputFile(self.output_filename),
                    mgd.TempFile('temp_space'))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, [os.path.join(pipeline_dir, 'tmp/temp_space')])

            def notworking_test_remove(self):

                workflow = pypeliner.workflow.Workflow()

                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                workflow.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                workflow.transform('write', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                failed = not scheduler.run(exec_queue)
                self.assertTrue(failed)
                
                os.remove(os.path.join(pipeline_dir, 'tmp/output_data'))
                
                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

            def test_simple_create_all(self):

                workflow = pypeliner.workflow.Workflow()

                # Read data into a managed object
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                workflow.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                workflow.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.TempOutputFile('output_file'))

                scheduler = self.create_scheduler()
                scheduler.prune = False
                scheduler.cleanup = False
                scheduler.run(workflow, exec_queue)

                with open(os.path.join(pipeline_dir, 'tmp/output_file'), 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/input_data._o')))
                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/output_data._o')))

            def test_cycle(self):

                workflow = pypeliner.workflow.Workflow()

                # Read data into a managed object, but also add a superfluous
                # input argument called `cyclic`, which is generated by a
                # downstream job
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename),
                    mgd.TempInputObj('cyclic'))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                workflow.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file, and also include `cyclic`
                # as an output so as to create a cycle in the dependency graph
                workflow.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename),
                    mgd.TempOutputObj('cyclic'))

                scheduler = self.create_scheduler()

                self.assertRaises(pypeliner.graph.DependencyCycleException, scheduler.run, workflow, exec_queue)

            def test_commandline_simple(self):

                workflow = pypeliner.workflow.Workflow()

                # Copy input to output file using linux `cat`
                workflow.commandline('do', (), self.ctx, 'cat',
                    mgd.InputFile(self.input_filename),
                    '>', mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

            def test_single_object_split(self):

                workflow = pypeliner.workflow.Workflow()

                # Read data into a managed object, which is a string
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Split the string into individual characters
                workflow.transform('splitbychar', (), self.ctx, split_stuff,
                    mgd.TempOutputObj('input_data', 'bychar'),
                    mgd.TempInputObj('input_data'))
                
                # Modify each single character string, appending `-`
                workflow.transform('do', ('bychar',), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data', 'bychar'),
                    mgd.TempInputObj('input_data', 'bychar').prop('some_string'))
                
                # Merge the modified strings
                workflow.transform('mergebychar', (), self.ctx, merge_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('output_data', 'bychar'))
                
                # Write the modified merged string to an output file
                workflow.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['l-i-n-e-1-\n', '-l-i-n-e-2-\n', '-l-i-n-e-3-\n', '-l-i-n-e-4-\n', '-l-i-n-e-5-\n', '-l-i-n-e-6-\n', '-l-i-n-e-7-\n', '-l-i-n-e-8-'])

            def test_change_axis(self):

                workflow = pypeliner.workflow.Workflow()

                # Read and modify a file and output to a temporary output file
                # with name `mod_input_filename`
                workflow.transform('dofilestuff', (), self.ctx, do_file_stuff,
                    None,
                    mgd.InputFile(self.input_filename),
                    mgd.TempOutputFile('mod_input_filename'),
                    'm')

                # Split the same input file by pairs of lines and output
                # two lines per file
                workflow.transform('splitbyline', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    2,
                    mgd.TempOutputFile('input_filename', 'byline'))
                
                # Do an identical split on `mod_input_filename`
                workflow.transform('splitbyline2', (), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('mod_input_filename'),
                    2,
                    mgd.TempOutputFile('mod_input_filename', 'byline2'))
                
                # Change the `mod_input_filename` split file to have the same axis as the first split
                workflow.changeaxis('changeaxis', (), 'mod_input_filename', 'byline2', 'byline')

                # Modify split versions of `input_filename` and `mod_input_filename` in tandem, 
                # concatenate both files together
                workflow.transform('dopairedstuff', ('byline',), self.ctx, do_paired_stuff,
                    None,
                    mgd.TempOutputFile('output_filename', 'byline'),
                    mgd.TempInputFile('input_filename', 'byline'),
                    mgd.TempInputFile('mod_input_filename', 'byline'))
                
                # Merge concatenated files and output
                workflow.transform('mergebychar', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_filename', 'byline'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.cleanup = False
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', '0mline1\n', '1mline2\n', 'line3\n', 'line4\n', '2mline3\n', '3mline4\n', 'line5\n', 'line6\n', '4mline5\n', '5mline6\n', 'line7\n', 'line8\n', '6mline7\n', '7mline8\n'])

            def test_split_getinstance(self):

                workflow = pypeliner.workflow.Workflow()

                # Split input file by line and output one file per line
                workflow.transform('splitbyline', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    1,
                    mgd.TempOutputFile('input_filename', 'byline'))

                # Append the `instance` of the split (basically the index of the line)
                # to each temporary file `input_filename` and output to a new file
                workflow.transform('append', ('byline',), self.ctx, append_to_lines_instance,
                    None,
                    mgd.TempInputFile('input_filename', 'byline'),
                    mgd.InputInstance('byline'),
                    mgd.TempOutputFile('output_filename', 'byline'))
                
                # Merge files and output
                workflow.transform('mergebyline', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_filename', 'byline'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line10\n', 'line21\n', 'line32\n', 'line43\n', 'line54\n', 'line65\n', 'line76\n', 'line87\n'])

            def test_split_getinstances(self):

                workflow = pypeliner.workflow.Workflow()

                # Split file by line and output a single line per temporary output
                # file named `input_filename`
                workflow.transform('splitbyline', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    1,
                    mgd.TempOutputFile('input_filename', 'byline'))

                # Write the list of chunks (line indexes) on the axis `byline` produced
                # by splitting the input file
                workflow.transform('writelist', (), self.ctx, write_list,
                    None,
                    mgd.InputChunks('byline'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['01234567'])

            def test_multiple_object_split(self):

                workflow = pypeliner.workflow.Workflow()

                # Read input file and store in managed input object, which is 
                # a string of the file contents
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Split the string by line and store as a new object
                workflow.transform('splitbyline', (), self.ctx, split_by_line,
                    mgd.TempOutputObj('input_data', 'byline'),
                    mgd.TempInputObj('input_data'))
                
                # Split each of the resulting strings by character and 
                # output as single character strings
                workflow.transform('splitbychar', ('byline',), self.ctx, split_by_char,
                    mgd.TempOutputObj('input_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline'))

                # Transform each single character string, appending a `-` character
                workflow.transform('do', ('byline', 'bychar'), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline', 'bychar').prop('some_string'))

                # Merge modified strings along the `bychar` axis
                workflow.transform('mergebychar', ('byline',), self.ctx, merge_stuff,
                    mgd.TempOutputObj('output_data', 'byline'),
                    mgd.TempInputObj('output_data', 'byline', 'bychar'))

                # Merge modified strings along the `byline` axis
                workflow.transform('mergebyline', (), self.ctx, merge_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('output_data', 'byline'))

                # Write the merged string to an output file
                workflow.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['l-i-n-e-1-l-i-n-e-2-l-i-n-e-3-l-i-n-e-4-l-i-n-e-5-l-i-n-e-6-l-i-n-e-7-l-i-n-e-8-'])

                workflow = pypeliner.workflow.Workflow()

                # Redo the same steps and ensure that each step is skipped because each
                # step is up to date
                workflow.transform('read', (), self.ctx, do_assert,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))
                workflow.transform('splitbyline', (), self.ctx, do_assert,
                    mgd.TempOutputObj('input_data', 'byline'),
                    mgd.TempInputObj('input_data'))
                workflow.transform('splitbychar', ('byline',), self.ctx, do_assert,
                    mgd.TempOutputObj('input_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline'))
                workflow.transform('do', ('byline', 'bychar'), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline', 'bychar').prop('some_string'))
                workflow.transform('mergebychar', ('byline',), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data', 'byline'),
                    mgd.TempInputObj('output_data', 'byline', 'bychar'))
                workflow.transform('mergebyline', (), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('output_data', 'byline'))
                workflow.transform('write', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

            def test_multiple_file_split(self):

                workflow = pypeliner.workflow.Workflow()

                # Split input file into 4 lines per output file (axis `byline_a`)
                workflow.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                workflow.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis
                workflow.transform('do', ('byline_a', 'byline_b'), self.ctx, do_file_stuff,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.InputInstance('byline_a'))

                # Merge along the `byline_b` axis
                workflow.transform('merge_byline_b', ('byline_a',), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                workflow.transform('merge_byline_a', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.cleanup = False
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['00line1\n', '10line2\n', '00line3\n', '10line4\n', '01line5\n', '11line6\n', '01line7\n', '11line8\n'])

                tmp_input_data_filenames = [os.path.join(pipeline_dir, 'tmp/byline_a/0/input_data'), os.path.join(pipeline_dir, 'tmp/byline_a/1/input_data')]
                tmp_data_checks = [['line1\n', 'line2\n', 'line3\n', 'line4\n'], ['line5\n', 'line6\n', 'line7\n', 'line8\n']]
                for tmp_input_data_filename, tmp_data_check in zip(tmp_input_data_filenames, tmp_data_checks):
                    with open(tmp_input_data_filename, 'r') as tmp_input_data_file:
                        tmp_input_data = tmp_input_data_file.readlines()
                        self.assertEqual(tmp_input_data, tmp_data_check)

            def test_rerun_simple(self):

                workflow = pypeliner.workflow.Workflow()

                self.assertFalse(os.path.exists(self.output_filename))

                # Modify input file, append `!` to each line
                workflow.transform('step1', (), self.ctx, append_to_lines,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))

                # Copy the file
                workflow.transform('step2', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))

                # This job should copy the file again but does nothing, raising
                # an exception when the pipeline is run
                workflow.transform('step3', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.cleanup = False
                self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue)

                workflow = pypeliner.workflow.Workflow()

                # Redo the previous steps, ensuring the first two steps are not
                # run since their dependencies are up to date, and make sure the final
                # copy is done correctly
                workflow.transform('step1', (), self.ctx, do_assert,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))
                workflow.transform('step2', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))
                workflow.transform('step3', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                scheduler.cleanup = True
                scheduler.run(workflow, exec_queue)

                # The temporary files should have been cleaned up
                self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))
                self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended_copy')))

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])

            def test_repopulate(self):

                workflow = pypeliner.workflow.Workflow()

                self.assertFalse(os.path.exists(self.output_filename))

                # Modify input file, append `!` to each line
                workflow.transform('step1', (), self.ctx, append_to_lines,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))

                # Copy the file
                workflow.transform('step2', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))

                # This job should copy the file again but does nothing, raising
                # an exception when the pipeline is run
                workflow.transform('step3', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue)

                self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))

                workflow = pypeliner.workflow.Workflow()

                # Rerun the same pipeline in repopulate mode, this time the final
                # copy is done correctly
                workflow.transform('step1', (), self.ctx, append_to_lines,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))
                workflow.transform('step2', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))
                workflow.transform('step3', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.repopulate = True
                scheduler.cleanup = False
                scheduler.run(workflow, exec_queue)

                # The temporary files should have been cleaned up
                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))
                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended_copy')))

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])


            def test_rerun_multiple_file_split(self):

                workflow = pypeliner.workflow.Workflow()

                # Split input file into 4 lines per output file (axis `byline_a`)
                workflow.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                workflow.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis, fail here
                workflow.transform('do', ('byline_a', 'byline_b'), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    '!',
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'))

                # Merge along the `byline_b` axis
                workflow.transform('merge_byline_b', ('byline_a',), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                workflow.transform('merge_byline_a', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue)

                workflow = pypeliner.workflow.Workflow()

                # Split input file into 4 lines per output file (axis `byline_a`)
                workflow.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                workflow.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis
                workflow.transform('do', ('byline_a', 'byline_b'), self.ctx, append_to_lines,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    '!',
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'))

                # Merge along the `byline_b` axis, fail here
                workflow.transform('merge_byline_b', ('byline_a',), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                workflow.transform('merge_byline_a', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                self.assertRaises(pypeliner.scheduler.PipelineException, scheduler.run, workflow, exec_queue)

                workflow = pypeliner.workflow.Workflow()

                # Split input file into 4 lines per output file (axis `byline_a`)
                workflow.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                workflow.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis
                workflow.transform('do', ('byline_a', 'byline_b'), self.ctx, append_to_lines,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    '!',
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'))

                # Merge along the `byline_b` axis
                workflow.transform('merge_byline_b', ('byline_a',), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                workflow.transform('merge_byline_a', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])


            def test_object_identical(self):

                workflow = pypeliner.workflow.Workflow()

                # Read data into a managed object
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                workflow.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                workflow.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.cleanup = False
                scheduler.run(workflow, exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

                workflow = pypeliner.workflow.Workflow()

                shutil.copyfile(self.input_filename, self.input_filename+'.tmp')

                # Read the same data into a managed object
                workflow.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename+'.tmp'))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                workflow.transform('do', (), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                workflow.transform('write', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                scheduler = self.create_scheduler()
                scheduler.run(workflow, exec_queue)


        unittest.main()

else:

    class stuff:
        def __init__(self, some_string):
            self.some_string = some_string

    def read_stuff(filename):
        with open(filename, 'r') as f:
            return stuff(''.join(f.readlines()).rstrip())

    def split_stuff(stf):
        return dict([(ind,stuff(value)) for ind,value in enumerate(list(stf.some_string))])

    def split_file_byline(in_filename, lines_per_file, out_filename_callback):
        with open(in_filename, 'r') as in_file:
            def line_group(line, line_idx=itertools.count()):
                return int(next(line_idx) / lines_per_file)
            for file_idx, lines in itertools.groupby(in_file, key=line_group):
                with open(out_filename_callback(file_idx), 'w') as out_file:
                    for line in lines:
                        out_file.write(line)

    def do_file_stuff(in_filename, out_filename, toadd):
        with open(in_filename, 'r') as in_file, open(out_filename, 'w') as out_file:
            line_number = 0
            for line in in_file:
                out_file.write(str(line_number) + str(toadd) + line.rstrip() + '\n')
                line_number += 1

    def merge_file_byline(in_filenames, out_filename):
        with open(out_filename, 'w') as out_file:
            for id, in_filename in sorted(in_filenames.items()):
                with open(in_filename, 'r') as in_file:
                    for line in in_file.readlines():
                        out_file.write(line)

    def split_by_line(stf):
        return dict([(ind,stuff(value)) for ind,value in enumerate(stf.some_string.split('\n'))])

    def split_by_char(stf):
        return dict([(ind,stuff(value)) for ind,value in enumerate(list(stf.some_string))])

    def do_stuff(a):
        return a + '-'

    def do_paired_stuff(output_filename, input1_filename, input2_filename):
        os.system('cat ' + input1_filename + ' ' + input2_filename + ' > ' + output_filename)

    def dict_arg_stuff(output_filenames, input_filenames):
        append_to_lines(input_filenames['1'], '1', output_filenames['1'])
        append_to_lines(input_filenames['2'], '2', output_filenames['2'])

    def merge_stuff(stfs):
        merged = ''
        for split, stf in sorted(stfs.iteritems()):
            merged = merged + stf
        return merged

    def write_stuff(a, filename):
        with open(filename, 'w') as f:
            f.write(a)

    def append_to_lines(in_filename, append, out_filename):
        with open(in_filename, 'r') as in_file, open(out_filename, 'w') as out_file:
            for line in in_file:
                out_file.write(line.rstrip() + append + '\n')

    def append_to_lines_instance(in_filename, instance, out_filename):
        with open(in_filename, 'r') as in_file, open(out_filename, 'w') as out_file:
            for line in in_file:
                out_file.write(line.rstrip() + str(instance) + '\n')

    def copy_file(in_filename, out_filename):
        with open(in_filename, 'r') as in_file, open(out_filename, 'w') as out_file:
            for line in in_file:
                out_file.write(line)

    def write_list(in_list, out_filename):
        with open(out_filename, 'w') as out_file:
            for a in sorted(in_list):
                out_file.write(str(a[0]))

    def do_nothing(*arg):
        pass

    def do_assert(*arg):
        assert False

    def set_chunks():
        return [1, 2]

    def file_transform(in_filename, out_filename, prefix, template_filename):
        with open(template_filename, 'w'):
            pass
        with open(in_filename, 'r') as in_file, open(out_filename, 'w') as out_file:
            for line in in_file:
                out_file.write('{0}'.format(prefix) + line)

    def write_files(out_filename_callback):
        for chunk in (1, 2):
            with open(out_filename_callback(chunk), 'w') as f:
                f.write('file{0}\n'.format(chunk))

    def check_temp(output_filename, temp_filename):
        with open(output_filename, 'w') as output_file:
            output_file.write(temp_filename)

    def create_workflow_2(input_filename, output_filename):
        workflow = pypeliner.workflow.Workflow()
        ctx = dict({'mem':1})

        workflow.transform('dofilestuff1', (), ctx, do_file_stuff,
            None,
            mgd.InputFile(input_filename),
            mgd.TempOutputFile('intermediate1'),
            'a')

        workflow.transform('dofilestuff2', (), ctx, do_file_stuff,
            None,
            mgd.TempInputFile('intermediate1'),
            mgd.OutputFile(output_filename),
            'b')

        return workflow

    def create_workflow_1(input_filename, output_filename):
        workflow = pypeliner.workflow.Workflow()
        ctx = dict({'mem':1})

        # Read data into a managed object
        workflow.transform('read', (), ctx, read_stuff,
            mgd.TempOutputObj('input_data'),
            mgd.InputFile(input_filename))

        # Extract a property of the managed object, modify it
        # and store the result in another managed object
        workflow.transform('do', (), ctx, do_stuff,
            mgd.TempOutputObj('output_data'),
            mgd.TempInputObj('input_data').prop('some_string'))

        # Write the object to an output file
        workflow.transform('write', (), ctx, write_stuff,
            None,
            mgd.TempInputObj('output_data'),
            mgd.TempOutputFile('output_file'))

        # Recursive workflow
        workflow.subworkflow('sub_workflow_2', (), create_workflow_2,
            mgd.TempInputFile('output_file'),
            mgd.OutputFile(output_filename))

        return workflow


