.. _test:

Testing in owmeta-core
======================

Preparing for tests
-------------------

owmeta_core should be installed like::

    pip install -e .

Running tests
-------------
Tests should be run via setup.py like::

    python setup.py test

you can pass options to ``pytest`` like so::

    python setup.py test --addopts '-k CommandTest'

Writing tests
-------------
Tests are written using Python's unittest. In general, a collection of
closely related tests should be in one file. For selecting different classes of
tests, tests can also be tagged using pytest marks like::

    @pytest.mark.tag
    class TestClass(unittest.TestCase):
        ...

Currently, marks are used to distinguish between unit-level tests and others
which have the ``inttest`` mark

Deselecting tests
-----------------
Tests can be deselected by adding a pytest `"marker"`_ to the test function,
class, or module and then adding ``-m 'not <your_marker>'`` to the pytest
command line. Marking tests to be explicitly deselected is preferred to
skipping tests since skipped tests tend to break silently, especially with
conditional skips such as with with ``pytest.mark.skipif``. A set of markers
is, however, deselected by default in the ``addopts`` line in our
``pytest.ini`` file. Deselected marks are added on a case-by-case basis and
will always run on CI.

.. _"marker": https://docs.pytest.org/en/latest/mark.html
