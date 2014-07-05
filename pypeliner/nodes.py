import os
import pickle

import helpers
import resources

def node_subdir(node):
    if len(node) == 0:
        return ''
    return os.path.join(*([os.path.join(*(str(axis), str(chunk))) for axis, chunk in node]))

def name_node_filename(name, node):
    assert not os.path.isabs(name)
    return os.path.join(node_subdir(node), name)

def name_node_displayname(name, node):
    return name + '<' + ','.join(':'.join((str(axis), str(chunk))) for axis, chunk in node) + '>'

class NodeManager(object):
    """ Manages nodes in the underlying pipeline graph """
    def __init__(self, nodes_dir, temps_dir):
        self.nodes_dir = nodes_dir
        self.temps_dir = temps_dir
        self.cached_chunks = dict()
    def retrieve_nodes(self, axes, base_node=()):
        if len(axes) == 0:
            yield base_node
        else:
            for chunk in self.retrieve_chunks(axes[0], base_node):
                for node in self.retrieve_nodes(axes[1:], base_node + ((axes[0], chunk),)):
                    yield node
    def get_chunks_filename(self, axis, node):
        return os.path.join(self.nodes_dir, node_subdir(node), axis+'_chunks')
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
            helpers.makedirs(os.path.join(self.temps_dir, node_subdir(node + ((axis, chunk),))))
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
