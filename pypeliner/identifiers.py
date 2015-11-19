import collections
import os

AxisInstanceBase = collections.namedtuple('AxisInstanceBase', ['axis', 'chunk'])

class AxisInstance(AxisInstanceBase):
    def __new__ (cls, axis, chunk):
        assert isinstance(axis, str)
        return super(AxisInstance, cls).__new__(cls, axis, chunk)
    @property
    def subdir(self):
        return os.path.join(self.axis, str(self.chunk))
    @property
    def displayname(self):
        return ':'.join([self.axis, str((self.chunk, '?')[self.chunk is None])])

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
