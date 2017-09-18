

class UnavailableError(Exception):
    pass


class FlyweightState(object):
    instances = {}
    def __init__(self, state_id, state_factory):
        self.state_id = state_id
        self.state_factory = state_factory
    def __enter__(self):
        if self.state_id in self.instances:
            raise Exception('Multiple FlyweightState instances with id {}'.format(self.state_id))
        self.instances[self.state_id] = self.state_factory()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
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
