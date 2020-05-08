import unittest

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock

import owmeta_core.cli as PCLI
from .TestUtilities import noexit, stdout
import json
import re


class CLIOutputModeTest(unittest.TestCase):
    def setUp(self):
        self.ccw = patch('owmeta_core.cli.CLICommandWrapper').start()

        class A(object):
            pass
        self.cmd = patch('owmeta_core.cli.OWM', new=A).start()
        patch('owmeta_core.cli.GitRepoProvider').start()

    def tearDown(self):
        patch.stopall()


class CLIJSONOutputModeTest(CLIOutputModeTest):
    def test_json_list(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'json'
                return ['a']
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(json.loads(so.getvalue()), ['a'])

    def test_json_set(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'json'
                return set('ab')
            self.ccw().main.side_effect = main
            PCLI.main()
        val = json.loads(so.getvalue())
        self.assertTrue(val == list('ba') or val == list('ab'))

    def test_json_context(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                from owmeta_core.context import Context
                argument_namespace_callback.output_mode = 'json'
                return Context('ident', base_namespace='base_namespace')
            self.ccw().main.side_effect = main
            PCLI.main()
        val = json.loads(so.getvalue())
        self.assertEqual(val, dict(identifier='ident', base_namespace='base_namespace'))

    def test_json_graph(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                from rdflib.graph import Graph
                argument_namespace_callback.output_mode = 'json'
                return Mock(name='graph', spec=Graph())
            self.ccw().main.side_effect = main
            PCLI.main()
        val = json.loads(so.getvalue())
        self.assertEqual(val, [])


class CLITextOutputModeTest(CLIOutputModeTest):
    def test_text_list(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                return ['a']
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), 'a\n')

    def test_text_multiple_element_list(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                return ['a', 'b']
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), 'a\nb\n')

    def test_text_set(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                return set('ab')
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertTrue(so.getvalue() == 'b\na\n' or so.getvalue() == 'a\nb\n')

    def test_text_dict(self):
        with noexit(), stdout() as so:

            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                return dict(a='b')
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), 'a\tb\n')

    def test_text_dict_field_separator(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                argument_namespace_callback.text_field_separator = '\0'
                return dict(a='b')
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), 'a\0b\n')

    def test_text_dict_record_separator(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                argument_namespace_callback.text_record_separator = '\0'
                return dict(a='b')
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), 'a\tb\0')

    def test_text_list_record_separator(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                argument_namespace_callback.text_record_separator = '\0'
                return ['a', 'b']
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), 'a\0b\0')

    def test_text_uniterable(self):
        target = object()
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'
                return target
            self.ccw().main.side_effect = main
            PCLI.main()
        self.assertEqual(so.getvalue(), str(target) + '\n')

    def test_text_iterable_type_error(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'text'

                def iterable():
                    yield 'blah'
                    yield 'blah'
                    raise TypeError("blah blah")
                return iterable()
            self.ccw().main.side_effect = main
            with self.assertRaises(TypeError):
                PCLI.main()
            self.assertEqual(so.getvalue(), 'blah\nblah\n')


class CLITableOutputModeTest(CLIOutputModeTest):
    def test_no_headers_or_columns_header_name(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'table'

                def iterable():
                    yield 'blah'
                    yield 'blah'
                return iterable()
            self.ccw().main.side_effect = main
            PCLI.main()
            self.assertRegexpMatches(so.getvalue(), 'Value')

    def test_no_headers_or_columns_row_value(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'table'

                def iterable():
                    yield 'blah'
                    yield 'blah'
                return iterable()
            self.ccw().main.side_effect = main
            PCLI.main()
            self.assertRegexpMatches(so.getvalue(), 'blah')

    def test_with_header_row(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'table'

                def gen():
                    yield 'blah'
                    yield 'blah'
                it = gen()

                class Iterable(object):
                    header = ['FIELD']

                    def __next__(self):
                        return next(it)

                    next = __next__

                    def __iter__(self):
                        return iter(it)

                return Iterable()
            self.ccw().main.side_effect = main
            PCLI.main()
            self.assertRegexpMatches(so.getvalue(), 'blah')

    def test_with_header_name(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'table'

                def gen():
                    yield 'blah'
                    yield 'blah'
                it = gen()

                class Iterable(object):
                    header = ['FIELD']

                    def __next__(self):
                        return next(it)

                    next = __next__

                    def __iter__(self):
                        return iter(it)

                return Iterable()
            self.ccw().main.side_effect = main
            PCLI.main()
            self.assertRegexpMatches(so.getvalue(), 'FIELD')

    def test_with_header_and_columns_accessor(self):
        with noexit(), stdout() as so:
            @with_defaults
            def main(argument_namespace_callback, **kwargs):
                argument_namespace_callback.output_mode = 'table'

                def gen():
                    yield 'blah'
                    yield 'blah'
                it = gen()

                class Iterable(object):
                    header = ['FIELD']
                    columns = [lambda x: x[:1]]

                    def __next__(self):
                        return next(it)

                    next = __next__

                    def __iter__(self):
                        return iter(it)

                return Iterable()
            self.ccw().main.side_effect = main
            PCLI.main()
            self.assertRegexpMatches(so.getvalue(), re.compile('^b *$', flags=re.MULTILINE))


def test_existing_hints_preserved():
    with patch('owmeta_core.cli.iter_entry_points') as iter_entry_points:
        existing_hints = {'blah.blah': {'myhint': 'isgood'}}
        entry_point = Mock()
        entry_point.load.return_value = {'blah.blah': {'myhint': 'isbad'}}
        iter_entry_points.return_value = [entry_point]
        augmented_hints = PCLI._gather_hints_from_entry_points(dict(**existing_hints))
        assert augmented_hints == existing_hints


def test_existing_hints_override_attempt_warns(caplog):
    with patch('owmeta_core.cli.iter_entry_points') as iter_entry_points:
        existing_hints = {'blah.blah': {'myhint': 'isgood'}}
        entry_point = Mock(name='my_entry_point')
        entry_point.load.return_value = {'blah.blah': {'myhint': 'isbad'}}
        iter_entry_points.return_value = [entry_point]
        PCLI._gather_hints_from_entry_points(dict(**existing_hints))
        assert 'my_entry_point' in caplog.text
        assert 'blah.blah' in caplog.text


def test_augmented_hints(caplog):
    with patch('owmeta_core.cli.iter_entry_points') as iter_entry_points:
        existing_hints = {'blah.blah': {'myhint': 'isgood'}}
        entry_point = Mock(name='my_entry_point')
        entry_point.load.return_value = {'blah.bluh': {'myhint': 'isalsogood'}}
        iter_entry_points.return_value = [entry_point]
        augmented_hints = PCLI._gather_hints_from_entry_points(dict(**existing_hints))
        assert augmented_hints == {
                'blah.blah': {'myhint': 'isgood'},
                'blah.bluh': {'myhint': 'isalsogood'}}


def with_defaults(func):
    '''
    Sets the default values for options
    '''
    from functools import wraps

    @wraps(func)
    def wrapper(argument_namespace_callback, argument_callback, *args, **kwargs):
        collect_argument_defaults(argument_namespace_callback, argument_callback)
        kwargs['argument_namespace_callback'] = argument_namespace_callback
        kwargs['argument_callback'] = argument_callback
        return func(*args, **kwargs)
    return wrapper


def collect_argument_defaults(ns, callback):
    res = dict()
    parser = Mock(name='parser')

    def cb(*args, **kwargs):
        da = kwargs.get('default')
        setattr(ns, args[0].strip('-').replace('-', '_'), da)
    parser.add_argument.side_effect = cb
    callback(parser)
    return res
