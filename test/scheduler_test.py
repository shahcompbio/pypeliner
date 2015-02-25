import unittest
import shutil
import glob
import os
import time
import logging
import itertools

import pypeliner
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

                self.sch = pypeliner.scheduler.Scheduler()
                self.sch.set_pipeline_dir(pipeline_dir)
                self.sch.max_jobs = 10

            def test_simple_chunks1(self):

                self.create_scheduler()

                # Write a set of output files indexed by axis `byfile`
                self.sch.transform('write_files', (), self.ctx, write_files,
                    None,
                    mgd.OutputFile(self.output_n_filename, 'byfile'))

                self.sch.run(exec_queue)

                for chunk in ('1', '2'):
                    with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                        output = output_file.readlines()
                        self.assertEqual(output, ['file{0}\n'.format(chunk)])

            def test_simple_chunks2(self):

                self.create_scheduler()

                # Directly set the chunks indexed by axis `byfile`
                self.sch.transform('set_chunks', (), self.ctx, set_chunks,
                    mgd.OutputChunks('byfile'))

                # Transform the input files indexed by axis `byfile` to output files
                # also indexed by axis `byfile`
                self.sch.transform('do', ('byfile',), self.ctx, file_transform,
                    None,
                    mgd.InputFile(self.input_n_filename, 'byfile'),
                    mgd.OutputFile(self.output_n_filename, 'byfile'),
                    mgd.InputInstance('byfile'),
                    mgd.Template(self.output_n_template, 'byfile'))
                
                # Merge output files indexed by axis `byfile` into a single output file
                self.sch.transform('merge', (), self.ctx, merge_file_byline,
                    None,
                    mgd.InputFile(self.output_n_filename, 'byfile'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                for chunk in ('1', '2'):
                    self.assertTrue(os.path.exists(self.output_n_template.format(**{'byfile':chunk})))
                    with open(self.output_n_filename.format(**{'byfile':chunk}), 'r') as output_file:
                        output = output_file.readlines()
                        self.assertEqual(output, [chunk + 'line' + str(line_num) + '\n' for line_num in range(1, 9)])

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()
                    self.assertEqual(output, [chunk + 'line' + str(line_num) + '\n' for chunk in ('1', '2') for line_num in range(1, 9)])

            def test_simple(self):

                self.create_scheduler()

                # Read data into a managed object
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                self.sch.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                self.sch.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

            def test_tempfile(self):

                self.create_scheduler()

                # Write the name of the temp file produced by pypeliner
                # into an output file
                self.sch.transform('write_files', (), self.ctx, check_temp,
                    None,
                    mgd.OutputFile(self.output_filename),
                    mgd.TempFile('temp_space'))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, [os.path.join(pipeline_dir, 'tmp/temp_space')])

            def notworking_test_remove(self):

                self.create_scheduler()

                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                self.sch.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                self.sch.transform('write', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                failed = not self.sch.run(exec_queue)
                self.assertTrue(failed)
                
                os.remove(os.path.join(pipeline_dir, 'tmp/output_data'))
                
                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

            def test_simple_create_all(self):

                self.create_scheduler()
                self.sch.prune = False
                self.sch.cleanup = False

                # Read data into a managed object
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                self.sch.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                self.sch.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.TempOutputFile('output_file'))

                self.sch.run(exec_queue)

                with open(os.path.join(pipeline_dir, 'tmp/output_file'), 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/input_data._o')))
                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/output_data._o')))

            def test_cycle(self):

                self.create_scheduler()

                # Read data into a managed object, but also add a superfluous
                # input argument called `cyclic`, which is generated by a
                # downstream job
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename),
                    mgd.TempInputObj('cyclic'))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                self.sch.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file, and also include `cyclic`
                # as an output so as to create a cycle in the dependency graph
                self.sch.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename),
                    mgd.TempOutputObj('cyclic'))

                self.assertRaises(pypeliner.graph.DependencyCycleException, self.sch.run, exec_queue)

            def test_commandline_simple(self):

                self.create_scheduler()

                # Copy input to output file using linux `cat`
                self.sch.commandline('do', (), self.ctx, 'cat',
                    mgd.InputFile(self.input_filename),
                    '>', mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8\n'])

            def test_single_object_split(self):

                self.create_scheduler()

                # Read data into a managed object, which is a string
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Split the string into individual characters
                self.sch.transform('splitbychar', (), self.ctx, split_stuff,
                    mgd.TempOutputObj('input_data', 'bychar'),
                    mgd.TempInputObj('input_data'))
                
                # Modify each single character string, appending `-`
                self.sch.transform('do', ('bychar',), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data', 'bychar'),
                    mgd.TempInputObj('input_data', 'bychar').prop('some_string'))
                
                # Merge the modified strings
                self.sch.transform('mergebychar', (), self.ctx, merge_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('output_data', 'bychar'))
                
                # Write the modified merged string to an output file
                self.sch.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['l-i-n-e-1-\n', '-l-i-n-e-2-\n', '-l-i-n-e-3-\n', '-l-i-n-e-4-\n', '-l-i-n-e-5-\n', '-l-i-n-e-6-\n', '-l-i-n-e-7-\n', '-l-i-n-e-8-'])

            def test_change_axis(self):

                self.create_scheduler()
                self.sch.cleanup = False

                # Read and modify a file and output to a temporary output file
                # with name `mod_input_filename`
                self.sch.transform('dofilestuff', (), self.ctx, do_file_stuff,
                    None,
                    mgd.InputFile(self.input_filename),
                    mgd.TempOutputFile('mod_input_filename'),
                    'm')

                # Split the same input file by pairs of lines and output
                # two lines per file
                self.sch.transform('splitbyline', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    2,
                    mgd.TempOutputFile('input_filename', 'byline'))
                
                # Do an identical split on `mod_input_filename`
                self.sch.transform('splitbyline2', (), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('mod_input_filename'),
                    2,
                    mgd.TempOutputFile('mod_input_filename', 'byline2'))
                
                # Change the `mod_input_filename` split file to have the same axis as the first split
                self.sch.changeaxis('changeaxis', (), 'mod_input_filename', 'byline2', 'byline')

                # Modify split versions of `input_filename` and `mod_input_filename` in tandem, 
                # concatenate both files together
                self.sch.transform('dopairedstuff', ('byline',), self.ctx, do_paired_stuff,
                    None,
                    mgd.TempOutputFile('output_filename', 'byline'),
                    mgd.TempInputFile('input_filename', 'byline'),
                    mgd.TempInputFile('mod_input_filename', 'byline'))
                
                # Merge concatenated files and output
                self.sch.transform('mergebychar', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_filename', 'byline'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', '0mline1\n', '1mline2\n', 'line3\n', 'line4\n', '2mline3\n', '3mline4\n', 'line5\n', 'line6\n', '4mline5\n', '5mline6\n', 'line7\n', 'line8\n', '6mline7\n', '7mline8\n'])

            def test_split_getinstance(self):

                self.create_scheduler()

                # Split input file by line and output one file per line
                self.sch.transform('splitbyline', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    1,
                    mgd.TempOutputFile('input_filename', 'byline'))

                # Append the `instance` of the split (basically the index of the line)
                # to each temporary file `input_filename` and output to a new file
                self.sch.transform('append', ('byline',), self.ctx, append_to_lines_instance,
                    None,
                    mgd.TempInputFile('input_filename', 'byline'),
                    mgd.InputInstance('byline'),
                    mgd.TempOutputFile('output_filename', 'byline'))
                
                # Merge files and output
                self.sch.transform('mergebyline', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_filename', 'byline'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line10\n', 'line21\n', 'line32\n', 'line43\n', 'line54\n', 'line65\n', 'line76\n', 'line87\n'])

            def test_split_getinstances(self):

                self.create_scheduler()

                # Split file by line and output a single line per temporary output
                # file named `input_filename`
                self.sch.transform('splitbyline', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    1,
                    mgd.TempOutputFile('input_filename', 'byline'))

                # Write the list of chunks (line indexes) on the axis `byline` produced
                # by splitting the input file
                self.sch.transform('writelist', (), self.ctx, write_list,
                    None,
                    mgd.InputChunks('byline'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['01234567'])

            def test_multiple_object_split(self):

                self.create_scheduler()

                # Read input file and store in managed input object, which is 
                # a string of the file contents
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Split the string by line and store as a new object
                self.sch.transform('splitbyline', (), self.ctx, split_by_line,
                    mgd.TempOutputObj('input_data', 'byline'),
                    mgd.TempInputObj('input_data'))
                
                # Split each of the resulting strings by character and 
                # output as single character strings
                self.sch.transform('splitbychar', ('byline',), self.ctx, split_by_char,
                    mgd.TempOutputObj('input_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline'))

                # Transform each single character string, appending a `-` character
                self.sch.transform('do', ('byline', 'bychar'), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline', 'bychar').prop('some_string'))

                # Merge modified strings along the `bychar` axis
                self.sch.transform('mergebychar', ('byline',), self.ctx, merge_stuff,
                    mgd.TempOutputObj('output_data', 'byline'),
                    mgd.TempInputObj('output_data', 'byline', 'bychar'))

                # Merge modified strings along the `byline` axis
                self.sch.transform('mergebyline', (), self.ctx, merge_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('output_data', 'byline'))

                # Write the merged string to an output file
                self.sch.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['l-i-n-e-1-l-i-n-e-2-l-i-n-e-3-l-i-n-e-4-l-i-n-e-5-l-i-n-e-6-l-i-n-e-7-l-i-n-e-8-'])

                self.create_scheduler()

                # Redo the same steps and ensure that each step is skipped because each
                # step is up to date
                self.sch.transform('read', (), self.ctx, do_assert,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))
                self.sch.transform('splitbyline', (), self.ctx, do_assert,
                    mgd.TempOutputObj('input_data', 'byline'),
                    mgd.TempInputObj('input_data'))
                self.sch.transform('splitbychar', ('byline',), self.ctx, do_assert,
                    mgd.TempOutputObj('input_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline'))
                self.sch.transform('do', ('byline', 'bychar'), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data', 'byline', 'bychar'),
                    mgd.TempInputObj('input_data', 'byline', 'bychar').prop('some_string'))
                self.sch.transform('mergebychar', ('byline',), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data', 'byline'),
                    mgd.TempInputObj('output_data', 'byline', 'bychar'))
                self.sch.transform('mergebyline', (), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('output_data', 'byline'))
                self.sch.transform('write', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

            def test_multiple_file_split(self):

                self.create_scheduler()

                # Split input file into 4 lines per output file (axis `byline_a`)
                self.sch.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                self.sch.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis
                self.sch.transform('do', ('byline_a', 'byline_b'), self.ctx, do_file_stuff,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.InputInstance('byline_a'))

                # Merge along the `byline_b` axis
                self.sch.transform('merge_byline_b', ('byline_a',), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                self.sch.transform('merge_byline_a', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                self.sch.cleanup = False

                self.sch.run(exec_queue)

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

                self.create_scheduler()

                self.assertFalse(os.path.exists(self.output_filename))

                # Modify input file, append `!` to each line
                self.sch.transform('step1', (), self.ctx, append_to_lines,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))

                # Copy the file
                self.sch.transform('step2', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))

                # This job should copy the file again but does nothing, raising
                # an exception when the pipeline is run
                self.sch.transform('step3', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                self.sch.cleanup = False
                self.assertRaises(pypeliner.scheduler.PipelineException, self.sch.run, exec_queue)

                self.create_scheduler()

                # Redo the previous steps, ensuring the first two steps are not
                # run since their dependencies are up to date, and make sure the final
                # copy is done correctly
                self.sch.transform('step1', (), self.ctx, do_assert,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))
                self.sch.transform('step2', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))
                self.sch.transform('step3', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                self.sch.cleanup = True
                self.sch.run(exec_queue)

                # The temporary files should have been cleaned up
                self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))
                self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended_copy')))

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])

            def test_repopulate(self):

                self.create_scheduler()

                self.assertFalse(os.path.exists(self.output_filename))

                # Modify input file, append `!` to each line
                self.sch.transform('step1', (), self.ctx, append_to_lines,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))

                # Copy the file
                self.sch.transform('step2', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))

                # This job should copy the file again but does nothing, raising
                # an exception when the pipeline is run
                self.sch.transform('step3', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                self.assertRaises(pypeliner.scheduler.PipelineException, self.sch.run, exec_queue)

                self.assertFalse(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))

                self.create_scheduler()

                # Rerun the same pipeline in repopulate mode, this time the final
                # copy is done correctly
                self.sch.transform('step1', (), self.ctx, append_to_lines,
                    None,
                    mgd.InputFile(self.input_filename),
                    '!',
                    mgd.TempOutputFile('appended'))
                self.sch.transform('step2', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended'),
                    mgd.TempOutputFile('appended_copy'))
                self.sch.transform('step3', (), self.ctx, copy_file,
                    None,
                    mgd.TempInputFile('appended_copy'),
                    mgd.OutputFile(self.output_filename))

                self.sch.repopulate = True
                self.sch.cleanup = False
                self.sch.run(exec_queue)

                # The temporary files should have been cleaned up
                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended')))
                self.assertTrue(os.path.exists(os.path.join(pipeline_dir, 'tmp/appended_copy')))

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])


            def test_rerun_multiple_file_split(self):

                self.create_scheduler()

                # Split input file into 4 lines per output file (axis `byline_a`)
                self.sch.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                self.sch.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis, fail here
                self.sch.transform('do', ('byline_a', 'byline_b'), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    '!',
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'))

                # Merge along the `byline_b` axis
                self.sch.transform('merge_byline_b', ('byline_a',), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                self.sch.transform('merge_byline_a', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                self.assertRaises(pypeliner.scheduler.PipelineException, self.sch.run, exec_queue)

                self.create_scheduler()

                # Split input file into 4 lines per output file (axis `byline_a`)
                self.sch.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                self.sch.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis
                self.sch.transform('do', ('byline_a', 'byline_b'), self.ctx, append_to_lines,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    '!',
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'))

                # Merge along the `byline_b` axis, fail here
                self.sch.transform('merge_byline_b', ('byline_a',), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                self.sch.transform('merge_byline_a', (), self.ctx, do_nothing,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                self.assertRaises(pypeliner.scheduler.PipelineException, self.sch.run, exec_queue)

                self.create_scheduler()

                # Split input file into 4 lines per output file (axis `byline_a`)
                self.sch.transform('split_byline_a', (), self.ctx, split_file_byline,
                    None,
                    mgd.InputFile(self.input_filename),
                    4,
                    mgd.TempOutputFile('input_data', 'byline_a'))

                # Split again, this time with 2 lines per output file (axis `byline_b`)
                self.sch.transform('split_byline_b', ('byline_a',), self.ctx, split_file_byline,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a'),
                    2,
                    mgd.TempOutputFile('input_data', 'byline_a', 'byline_b'))

                # Modify each file independently, adding the instance of this job on the
                # `byline_a` axis
                self.sch.transform('do', ('byline_a', 'byline_b'), self.ctx, append_to_lines,
                    None,
                    mgd.TempInputFile('input_data', 'byline_a', 'byline_b'),
                    '!',
                    mgd.TempOutputFile('output_data', 'byline_a', 'byline_b'))

                # Merge along the `byline_b` axis
                self.sch.transform('merge_byline_b', ('byline_a',), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a', 'byline_b'),
                    mgd.TempOutputFile('output_data', 'byline_a'))

                # Merge along the `byline_a` axis
                self.sch.transform('merge_byline_a', (), self.ctx, merge_file_byline,
                    None,
                    mgd.TempInputFile('output_data', 'byline_a'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1!\n', 'line2!\n', 'line3!\n', 'line4!\n', 'line5!\n', 'line6!\n', 'line7!\n', 'line8!\n'])


            def test_object_identical(self):

                self.create_scheduler()

                # Read data into a managed object
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                self.sch.transform('do', (), self.ctx, do_stuff,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                self.sch.transform('write', (), self.ctx, write_stuff,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                self.sch.cleanup = False
                self.sch.run(exec_queue)

                with open(self.output_filename, 'r') as output_file:
                    output = output_file.readlines()

                self.assertEqual(output, ['line1\n', 'line2\n', 'line3\n', 'line4\n', 'line5\n', 'line6\n', 'line7\n', 'line8-'])

                self.create_scheduler()

                shutil.copyfile(self.input_filename, self.input_filename+'.tmp')

                # Read the same data into a managed object
                self.sch.transform('read', (), self.ctx, read_stuff,
                    mgd.TempOutputObj('input_data'),
                    mgd.InputFile(self.input_filename+'.tmp'))

                # Extract a property of the managed object, modify it
                # and store the result in another managed object
                self.sch.transform('do', (), self.ctx, do_assert,
                    mgd.TempOutputObj('output_data'),
                    mgd.TempInputObj('input_data').prop('some_string'))

                # Write the object to an output file
                self.sch.transform('write', (), self.ctx, do_assert,
                    None,
                    mgd.TempInputObj('output_data'),
                    mgd.OutputFile(self.output_filename))

                self.sch.run(exec_queue)


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
                out_file.write(str(a))

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





