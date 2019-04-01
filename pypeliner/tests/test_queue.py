from __future__ import absolute_import

import argparse
import os
import logging

import pypeliner.execqueue.factory
import pypeliner.helpers


class BasicJob():
    def __init__(self):
        self.success = False
        self.ctx = {}
    def __call__(self):
        self.success = True


def run_basic(exec_queue, base_temps_dir):

    assert exec_queue.empty

    job = test_queue.BasicJob()
    temps_dir = os.path.join(base_temps_dir, 'basic')
    pypeliner.helpers.makedirs(temps_dir)
    exec_queue.send({'mem': 1}, 'basic', job, temps_dir)

    assert exec_queue.length == 1
    assert not exec_queue.empty

    name = exec_queue.wait()

    assert name == 'basic'

    recieved = exec_queue.receive(name)

    assert recieved.success == True

    print ('success')


if __name__ == '__main__':
    from . import test_queue

    argparser = argparse.ArgumentParser()

    argparser.add_argument('submit',
        help='Execution queue to test')

    argparser.add_argument('--nativespec', default=None,
        help='Native spec if needed')

    args = vars(argparser.parse_args())

    logging.basicConfig(level=logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(pypeliner.helpers.MultiLineFormatter('%(asctime)s - %(name)s - %(levelname)s - '))
    logging.getLogger('').addHandler(console)

    script_directory = os.path.dirname(os.path.abspath(__file__))
    base_temps_dir = os.path.join(script_directory, 'queue_test')

    exec_queue = pypeliner.execqueue.factory.create(args['submit'], [test_queue], native_spec=args['nativespec'])

    with exec_queue:
        run_basic(exec_queue, base_temps_dir)
