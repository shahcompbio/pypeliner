""" Reattachable flyweight objects

The reattachable flyweight objects are used for persisting state
that transitions through a series of pickling operations.  After unpickling,
state objects are reattached to an owner automatically.  This allows a state
object to be pickled, updated in a remote process, repickled, and then
unpickled in the original process updating the state to the value set
remotely.
"""

import uuid


class UnavailableError(Exception):
    pass


class FlyweightState(object):
    """ Collection of state objects that reattach upon unpickling.
    """
    instances = {}

    def __init__(self, state_container=None):
        if state_container is None:
            state_container = dict()
        self.state_id = str(uuid.uuid4())
        self.instances[self.state_id] = state_container

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.state_id in self.instances:
            del self.instances[self.state_id]

    def __getstate__(self):
        return (self.state_id,)

    def __setstate__(self, state):
        self.state_id, = state

    def create_flyweight(self, key):
        return ReattachableFlyweight(self, key)

    def set(self, key, value):
        if self.state_id not in self.instances:
            raise UnavailableError()
        self.instances[self.state_id][key] = value

    def get(self, key):
        if self.state_id not in self.instances:
            raise UnavailableError()
        return self.instances[self.state_id].get(key)


class ReattachableFlyweight(object):
    """ Reattachable state object.
    """

    def __init__(self, state, key):
        self.state = state
        self.key = key

    def __getstate__(self):
        saved = None
        try:
            saved = getattr(self, 'saved')
        except AttributeError:
            pass
        return (self.state, self.key, saved)

    def __setstate__(self, state):
        self.state, self.key, saved = state
        if saved is not None:
            self.set(saved)

    def set(self, value):
        try:
            self.state.set(self.key, value)
        except UnavailableError:
            self.saved = value

    def get(self):
        try:
            return self.state.get(self.key)
        except UnavailableError:
            pass
        if hasattr(self, 'saved'):
            return self.saved
