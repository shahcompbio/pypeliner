import collections
import os
import pickle

import helpers
import resources
import managed

AxisInstanceBase = collections.namedtuple('AxisInstanceBase', ['axis', 'chunk'])

class AxisInstance(AxisInstanceBase):
    @property
    def subdir(self):
        return os.path.join(str(self.axis), str(self.chunk))
    @property
    def displayname(self):
        return ':'.join([str(self.axis), str((self.chunk, '?')[self.chunk is None])])

class Namespace(str):
    @property
    def subdir(self):
        return self
    @property
    def displayname(self):
        return self

class Node(tuple):
    def __add__(self, a):
        if isinstance(a, (AxisInstance, Namespace)):
            return Node(self + Node([a]))
        elif isinstance(a, Node):
            return Node(super(Node, self).__add__(a))
        else:
            raise ValueError('Invalid type ' + str(type(a)) + ' for addition')
    def __getitem__(self, key):
        if isinstance(key, slice):
            return Node(super(Node, self).__getitem__(key))
        return super(Node, self).__getitem__(key)
    def __getslice__(self, i, j):
        return self.__getitem__(slice(i, j))
    @property
    def subdir(self):
        if len(self) == 0:
            return ''
        return os.path.join(*([a.subdir for a in self]))
    @property
    def displayname(self):
        return '/'.join([a.displayname for a in self])

class NodeManager(object):
    """ Manages nodes in the underlying pipeline graph """
    def __init__(self, nodes_dir, temps_dir):
        self.nodes_dir = nodes_dir
        self.temps_dir = temps_dir
        self.cached_chunks = dict()
    def retrieve_nodes(self, axes, base_node=None):
        if base_node is None:
            base_node = Node()
        assert isinstance(base_node, Node)
        if len(axes) == 0:
            yield base_node
        else:
            for chunk in self.retrieve_chunks(axes[0], base_node):
                for node in self.retrieve_nodes(axes[1:], base_node + AxisInstance(axes[0], chunk)):
                    yield node
    def get_chunks_filename(self, axis, node):
        return os.path.join(self.nodes_dir, node.subdir, axis+'_chunks')
    def retrieve_chunks(self, axis, node):
        if (axis, node) not in self.cached_chunks:
            chunks_filename = self.get_chunks_filename(axis, node)
            if not os.path.exists(chunks_filename):
                return (None,)
            else:
                with open(chunks_filename, 'rb') as f:
                    self.cached_chunks[(axis, node)] = pickle.load(f)
        return self.cached_chunks[(axis, node)]
    def store_chunks(self, axis, node, chunks):
        for chunk in chunks:
            new_node = node + AxisInstance(axis, chunk)
            helpers.makedirs(os.path.join(self.temps_dir, new_node.subdir))
        chunks = sorted(chunks)
        self.cached_chunks[(axis, node)] = chunks
        chunks_filename = self.get_chunks_filename(axis, node)
        helpers.makedirs(os.path.dirname(chunks_filename))
        temp_chunks_filename = chunks_filename + '.tmp'
        with open(temp_chunks_filename, 'wb') as f:
            pickle.dump(chunks, f)
        helpers.overwrite_if_different(temp_chunks_filename, chunks_filename)
    def get_merge_input(self, axis, node):
        return resources.ChunksResource(self, axis, node)
    def get_split_output(self, axis, node):
        return resources.Dependency(axis, node)
    def get_node_inputs(self, node):
        if len(node) >= 1:
            yield resources.Dependency(node[-1][0], node[:-1])
