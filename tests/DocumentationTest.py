# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import print_function
import unittest
import doctest
import os
from os.path import join as p
import tempfile
import shutil
import pytest
from .doctest_plugin import ALLOW_UNICODE, UnicodeOutputChecker


doctest.OutputChecker = UnicodeOutputChecker


@pytest.mark.inttest
class READMETest(unittest.TestCase):
    ''' Executes doctests '''
    def setUp(self):
        self.startdir = os.getcwd()
        self.testdir = tempfile.mkdtemp(prefix=__name__ + '.')
        shutil.copyfile('README.md', p(self.testdir, 'README.md'))
        shutil.copyfile('readme.conf', p(self.testdir, 'readme.conf'))
        os.chdir(self.testdir)

    def tearDown(self):
        os.chdir(self.startdir)
        shutil.rmtree(self.testdir)

    def test_readme(self):
        [failure_count, return_count] = doctest.testfile("README.md", module_relative=False,
                                                         optionflags=(ALLOW_UNICODE | doctest.ELLIPSIS))
        self.assertEqual(failure_count, 0)


def test_collection():
    from owmeta_core import collections
    [failure_count, return_count] = doctest.testmod(collections, optionflags=(ALLOW_UNICODE | doctest.ELLIPSIS))
    assert failure_count == 0


def test_bundle(custom_bundle):
    from owmeta_core import bundle
    from owmeta_core.context import Context
    from owmeta_core.dataobject import DataObject
    from owmeta_core.bundle import Descriptor, Bundle

    desc = Descriptor.load('''
    id: example/bundleId
    version: 42
    includes:
        - http://example.org/test_bundle
    ''')

    ctx = Context('http://example.org/test_bundle')
    ctx(DataObject)(ident='http://example.org/entities#aDataObject')

    with custom_bundle(desc, graph=ctx.rdf_graph()) as bun:
        class CustomBundle(Bundle):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, bundles_directory=bun.bundles_directory, **kwargs)

        [failure_count, return_count] = doctest.testmod(bundle,
                optionflags=(ALLOW_UNICODE | doctest.ELLIPSIS),
                extraglobs=dict(Bundle=CustomBundle, DataObject=DataObject))
    assert failure_count == 0


class SphinxTest(unittest.TestCase):
    ''' Executes doctests in Sphinx documentation '''
    def setUp(self):
        self.startdir = os.getcwd()
        self.testdir = tempfile.mkdtemp(prefix=__name__ + '.')
        shutil.copytree('docs', p(self.testdir, 'docs'))
        os.chdir(self.testdir)

    def tearDown(self):
        os.chdir(self.startdir)
        shutil.rmtree(self.testdir)

    def execute(self, fname, **kwargs):
        failure_count, _ = doctest.testfile(p('docs', fname + '.rst'), module_relative=False,
                optionflags=(ALLOW_UNICODE | doctest.ELLIPSIS), **kwargs)
        self.assertEqual(failure_count, 0)

    def test_making_dataObjects(self):
        self.execute('making_dataObjects')

    def test_transactions(self):
        self.execute('transactions')

    def test_datasource(self):
        examples_dir = p(self.testdir, 'examples')
        os.mkdir(examples_dir)
        open(p(examples_dir, '__init__.py'), 'w').close()
        shutil.copytree(p(self.startdir, 'examples', 'datasource'), p(examples_dir, 'datasource'))
        self.execute('datasource')
