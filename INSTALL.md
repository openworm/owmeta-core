Installation
============
The recommended way to get owmeta-core is with [pip](http://pip.readthedocs.org/en/latest/installing.html)::

    pip install owmeta-core

Alternatively, you can grab the latest on the development branch from GitHub::

    git clone https://github.com/openworm/owmeta-core.git
    cd owmeta-core
    pip install -e .

Running tests
-------------

After checking out the project, tests can be run from the command line in the root folder with::

    pytest

You may also run individual test cases with::

    pytest -k <NameOfTest>

For example, to run ``test_rdfs_comment_property``::

    pytest -k test_rdfs_comment_property

Uninstall
----------

To uninstall owmeta-core::

    pip uninstall owmeta-core
