.. _api:

API Reference
=============

Below we document the interface classes and functions of pypeliner.

Scheduler Object
----------------

.. automodule:: pypeliner.scheduler

.. autoclass:: Scheduler
    :members:

Managed Objects
---------------

.. automodule:: pypeliner.managed

.. autoclass:: Template
.. autoclass:: TempFile
.. autoclass:: InputFile
.. autoclass:: OutputFile
.. autoclass:: TempInputObj
    :members: prop, extract
.. autoclass:: TempOutputObj
.. autoclass:: TempInputFile
.. autoclass:: TempOutputFile
.. autoclass:: InputInstance
.. autoclass:: InputChunks
.. autoclass:: OutputChunks

Pypeline Object
---------------

.. automodule:: pypeliner.app

.. autofunction:: add_arguments

.. autoclass:: Pypeline
    :members:

Command Line Helper
-------------------

.. automodule:: pypeliner.commandline
    :members:

