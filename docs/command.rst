.. _command:

``owm`` Command Line
====================

The ``owm`` command line provides a high-level interface for working with
owmeta-core-managed data. The central object which ``owm`` works on is the
owmeta-core project, which contains the triple store -- a set of files in a
binary format. The sub-commands act on important files inside the project
or with entities in the database.

To get usage information::
   
   owm --help

To clone a project::

   owm clone $database_url

This will clone a project into ``.owm`` in your current working directory.
After a successful clone, a binary database usable as a owmeta store will have
been created from the serialized graphs (i.e., sets of |RDF| triples) in the
project.

To save changes made to the database, run the ``commit`` sub-command like this::

   owm commit -m "Adding records from January-March"

To recreate the database from serialized graphs, run the ``regendb`` sub-command::

   owm regendb

Be careful with ``regendb`` as it will delete anything you have added to binary
database beyond what's in the serialized graphs.

To make a new project::

   owm init

This will create a project in ``.owm`` in your current working directory.
