import unittest

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock

from pytest import raises
from owmeta_core.command_util import SubCommand, IVar
from owmeta_core.cli_command_wrapper import CLICommandWrapper, CLIArgMapper
from owmeta_core.cli_common import METHOD_NAMED_ARG, METHOD_NARGS, METHOD_KWARGS
from .TestUtilities import noexit, stdout


class CLICommandWrapperTest(unittest.TestCase):

    def test_method_sc(self):
        class A(object):
            def __init__(self):
                self.i = 0

            def sc(self):
                self.i += 1
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        parser.parse_args(['sc'])
        cm.mapper.apply(a)
        self.assertEqual(a.i, 1)

    def test_method_sc_doc(self):
        class A(object):
            def __init__(self):
                self.i = 0

            def sc(self):
                ''' TEST_STRING '''
                self.i += 1
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        self.assertIn('TEST_STRING', parser.format_help())

    def test_method_sc_doc_param(self):
        class A(object):
            def __init__(self):
                self.i = 0

            def sc(self, opt):
                '''
                Test

                Parameters
                ----------
                opt : str
                    TEST_STRING
                '''
                self.i += 1
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        with noexit(), stdout() as out:
            parser.parse_args(['sc', '--help'])
        self.assertIn('TEST_STRING', out.getvalue())

    def test_method_sc_nargs(self):
        class A(object):
            def __init__(self):
                self.i = 0

            def sc(self, opt):
                '''
                Test

                Parameters
                ----------
                *opt : str
                    _
                '''
                self.i += 1
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        with noexit(), stdout() as out:
            parser.parse_args(['sc', '--help'])
        self.assertIn('opt ...', out.getvalue())

    def test_subcommand_sc(self):
        class S(object):
            def __init__(self, parent):
                self._parent = parent

            def __call__(self):
                self._parent.i = 1

        class A(object):
            def __init__(self):
                self.i = 0
            sc = SubCommand(S)

        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        parser.parse_args(['sc'])
        cm.mapper.apply(a)

        self.assertEqual(a.i, 1)

    def test_ivar_default_str(self):
        class A(object):
            p = IVar(3)
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        with noexit(), stdout() as out:
            parser.parse_args(['sc', '--help'])
        self.assertIn('3', out.getvalue())

    def test_ivar_default_append(self):
        class A(object):
            p = IVar(3, doc='TEST_STRING')
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        with noexit(), stdout() as out:
            parser.parse_args(['sc', '--help'])
        self.assertIn('3', out.getvalue())

    def test_ivar_default_append_doc(self):
        class A(object):
            p = IVar(3, doc='TEST_STRING')
        a = A()
        cm = CLICommandWrapper(a)
        parser = cm.parser()
        with noexit(), stdout() as out:
            parser.parse_args(['sc', '--help'])
        self.assertIn('TEST_STRING', out.getvalue())


class CLIArgMapperTest(unittest.TestCase):
    def test_nargs_with_named_args(self):
        cut = CLIArgMapper()
        cut.mappings[(METHOD_NAMED_ARG, 'name0', 0)] = 4
        cut.mappings[(METHOD_NARGS, 'name1', -1)] = [4, 5, 5]

        runner = Mock()
        cut.apply(runner)
        runner.assert_called_with(4, 4, 5, 5)

    def test_named_args_multiple(self):
        cut = CLIArgMapper()
        cut.mappings[(METHOD_NAMED_ARG, 'name0', 0)] = 4
        cut.mappings[(METHOD_NAMED_ARG, 'name1', 1)] = 6

        runner = Mock()
        cut.apply(runner)
        runner.assert_called_with(4, 6)

    def test_named_fallback_to_kwargs(self):
        cut = CLIArgMapper()
        cut.mappings[(METHOD_NAMED_ARG, 'name0', 0)] = 4
        cut.mappings[(METHOD_NAMED_ARG, 'name1', 2)] = 6

        runner = Mock()
        cut.apply(runner)
        runner.assert_called_with(name0=4, name1=6)

    def test_named_insufficient_args_error(self):
        cut = CLIArgMapper()
        cut.named_arg_count = 3
        cut.mappings[(METHOD_NAMED_ARG, 'name0', 0)] = 4
        cut.mappings[(METHOD_NAMED_ARG, 'name1', 1)] = 6

        runner = Mock()

        with raises(Exception):
            cut.apply(runner)

    def test_kwargs(self):
        cut = CLIArgMapper()
        cut.mappings[(METHOD_KWARGS, 'name0', -1)] = ['a=b', 'c=d']

        runner = Mock()

        cut.apply(runner)

        runner.assert_called_with(a='b', c='d')
