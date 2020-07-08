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
