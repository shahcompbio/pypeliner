import itertools
import os
import time

import pypeliner
import pypeliner.workflow
import pypeliner.managed as mgd


class stuff:
    def __init__(self, some_string):
        self.some_string = some_string

def read_stuff(filename):
    with open(filename, 'r') as f:
        return stuff(''.join(f.readlines()).rstrip())

def split_stuff(stf):
    return dict([(ind,stuff(value)) for ind,value in enumerate(list(stf.some_string))])

def split_file_byline(in_filename, lines_per_file, out_filename_callback, max_files=None):
    with open(in_filename, 'r') as in_file:
        def line_group(line, line_idx=itertools.count()):
            return int(next(line_idx) / lines_per_file)
        for file_idx, lines in itertools.groupby(in_file, key=line_group):
            if max_files is not None and file_idx >= max_files:
                break
            with open(out_filename_callback(file_idx), 'w') as out_file:
                for line in lines:
                    out_file.write(line)

def split_2_file_byline(in_filename, out_filename_1, out_filename_2):
    split_file_byline(in_filename, 2, out_filename_1)
    split_file_byline(in_filename, 2, out_filename_2)

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

def merge_2_file_byline(in_filenames_1, in_filenames_2, out_filename):
    with open(out_filename, 'w') as out_file:
        for in_filenames in (in_filenames_1, in_filenames_2):
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
    for split, stf in sorted(stfs.items()):
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

def file_transform(in_filename, out_filename, prefix, template_filename, merge_templates):
    with open(template_filename, 'w'):
        pass
    with open(in_filename, 'r') as in_file, open(out_filename, 'w') as out_file:
        for key, value in merge_templates.items():
            out_file.write('{0}\t{1}\n'.format(key, value))
        for line in in_file:
            out_file.write('{0}'.format(prefix) + line)

def write_files(out_filename_callback):
    for chunk in (1, 2):
        with open(out_filename_callback(chunk), 'w') as f:
            f.write('file{0}\n'.format(chunk))

def check_temp(output_filename, temp_filename):
    with open(temp_filename, 'w'):
        pass
    with open(output_filename, 'w') as output_file:
        output_file.write(temp_filename)

def create_workflow_2(input_filename, output_filename):
    workflow = pypeliner.workflow.Workflow()

    workflow.transform(
        name='dofilestuff1',
        func='pypeliner.tests.tasks.do_file_stuff',
        args=(
            mgd.InputFile(input_filename),
            mgd.TempOutputFile('intermediate1'),
            'a'))

    workflow.transform(
        name='dofilestuff2',
        func='pypeliner.tests.tasks.do_file_stuff',
        args=(
            mgd.TempInputFile('intermediate1'),
            mgd.OutputFile(output_filename),
            'b'))

    return workflow

def create_workflow_1(input_filename, output_filename):
    workflow = pypeliner.workflow.Workflow()

    # Read data into a managed object
    workflow.transform(
        name='read',
        func='pypeliner.tests.tasks.read_stuff',
        ret=mgd.TempOutputObj('input_data'),
        args=(mgd.InputFile(input_filename),))

    # Extract a property of the managed object, modify it
    # and store the result in another managed object
    workflow.transform(
        name='do',
        func='pypeliner.tests.tasks.do_stuff',
        ret=mgd.TempOutputObj('output_data'),
        args=(mgd.TempInputObj('input_data').prop('some_string'),))

    # Write the object to an output file
    workflow.transform(
        name='write',
        func='pypeliner.tests.tasks.write_stuff',
        args=(
            mgd.TempInputObj('output_data'),
            mgd.TempOutputFile('output_file')))

    # Recursive workflow
    workflow.subworkflow(
        name='sub_workflow_2',
        func='pypeliner.tests.tasks.create_workflow_2',
        args=(
            mgd.TempInputFile('output_file'),
            mgd.OutputFile(output_filename)))

    return workflow


def touch(f):
    with open(f, 'w'):
        pass
        
        
def checkexists(f):
    assert os.path.exists(f)


def job1(i1, o1, o2, o3):
    checkexists(i1)
    touch(o1)
    touch(o2)
    touch(o3)
    time.sleep(1)


def job2(i1):
    checkexists(i1)
    time.sleep(1)
    return 'data'


def job3(i1, o1):
    checkexists(i1)
    time.sleep(1)
    touch(o1)


def job4(i1, i2, i3, o1):
    checkexists(i1)
    checkexists(i2)
    touch(o1)
    time.sleep(1)


def job5(i1, o1):
    checkexists(i1)
    touch(o1)
    time.sleep(1)


class Test(object):
    fragment_mean = None
    fragment_stddev = None
    
def calculate_fragment_stats(i1):
    checkexists(i1)
    return Test()
    

def sample_gc(o1, i1, i2, i3, i4):
    checkexists(i1)
    touch(o1)


def gc_lowess(i1, o1, o2):
    checkexists(i1)
    touch(o1)
    touch(o2)


def split_table(o1, i1, i2):
    checkexists(i1)
    touch(o1[0])
    touch(o1[1])
    touch(o1[2])


def gc_map_bias(i1, i2, i3, i4, o1, i5, i6):
    checkexists(i1)
    checkexists(i4)
    touch(o1)


def merge_tables(o1, i1):
    touch(o1)


def biased_length(o1, i1):
    checkexists(i1)
    touch(o1)

