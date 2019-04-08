.. pypeliner documentation master file, created by
   sphinx-quickstart on Wed Jun 18 21:03:59 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pypeliner's documentation!
=====================================

Pypeliner is a library for creating informatic workflows or pipelines.  It aims to reduce the amount boilerplate code
required to write a robust pipeline.

Features:

 * parallel execution of independent jobs
 * automatic parametric job creation and execution for massively parallel jobs
 * integration with grid engine and other cluster systems
 * temporary file management including on the fly removal of intermediate files
 * dependency resolution, rerun only out of date jobs
 * comprehensive logging

Contents:

.. toctree::
   :maxdepth: 2
   :glob:

   installation
   concepts
   tutorial
   api
   azure


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
