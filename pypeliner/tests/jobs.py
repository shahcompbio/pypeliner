
class TestJob(object):
    def __init__(self):
        self.called = False
    def __call__(self):
        print ('called')
        self.called = True
