import sys

_pypeliner_internal_global_state = {}

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


if sys.version_info[0] < 3:
    __all__ = ['helpers', 'scheduler', 'commandline', 'execqueue', 'delegator', 'app']
    import helpers
    import scheduler
    import execqueue
    import commandline
    import delegator
    import app
else:
    __path__ = __import__('pkgutil').extend_path(__path__, __name__)
