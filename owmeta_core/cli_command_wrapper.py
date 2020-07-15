from __future__ import print_function
import sys
import types
import argparse
import copy as _copy
import functools

from .utils import FCN
from .docscrape import parse as npdoc_parse
from .command_util import IVar, SubCommand


# TODO: Use `inspect` module for getting argument names so we aren't depending on
# docstrings
from .cli_common import (INSTANCE_ATTRIBUTE,
                         METHOD_NAMED_ARG,
                         METHOD_NARGS,
                         METHOD_KWARGS)

from .cli_hints import CLI_HINTS


ARGUMENT_TYPES = {
    'int': int
}
''' Map from parameter types to type constructors for parsing arguments '''


class CLIUserError(Exception):
    '''
    An error which the user would have to correct.

    Typically caused by invalid user input
    '''


def _method_runner(runner, key):
    method = getattr(runner, key)

    @functools.wraps(method)
    def _f(*args, **kwargs):
        return method(*args, **kwargs)
    return _f


def _sc_runner(sub_mapper, sub_runner):
    def _f():
        return sub_mapper.apply(sub_runner)
    return _f


class CLIArgMapper(object):
    '''
    Stores mappings for arguments and maps them back to the part of the object
    they come from
    '''
    def __init__(self):
        self.mappings = dict()
        self.methodname = None
        self.runners = dict()
        ''' Mapping from subcommand names to functions which run for them '''

        self.named_arg_count = dict()

        self.argparser = None
        # A special little mapper just for callable runners
        self.runner_mapper = None

    def apply(self, runner):
        '''
        Applies the collected arguments to the runner by calling methods and traversing
        the object attributes as required

        Parameters
        ----------
        runner : object
            Target of the command and source of argument and method names

        See Also
        --------
        CLICommandWrapper : accepts a runner argument in its ``__init__`` method
        '''
        iattrs = self.get(INSTANCE_ATTRIBUTE)
        kvpairs = self.get(METHOD_KWARGS)
        kvs = list(kv.split('=') for kv in next(iter(kvpairs.values()), ()))

        kwargs = {k: v for k, v in kvs}

        args = self.get_list(METHOD_NAMED_ARG)
        if not args:
            kwargs.update(self.get(METHOD_NAMED_ARG))

        try:
            # There is, at most, one nargs entry.
            nargs = next(iter(self.get(METHOD_NARGS).values()))
        except StopIteration:
            nargs = ()

        runmethod = self.runners.get(self.methodname, None)

        def continuation():
            argcount = self.named_arg_count.get(self.methodname)
            if nargs and args and argcount is not None and len(args) != argcount:
                # This means we have passed in positional arguments, and we have a
                # variable-length option, but we have not filled out all of the arguments
                # necessary to cleanly apply the runmethod since Python would think we're
                # trying to apply some arguments twice.
                #
                # We *could* support a slightly richer set of options here, but it's probably
                # not worth it...
                #
                # Also, this is a programmer error. End-users shouldn't hit this
                raise Exception('Missing arguments to method ' + str(self.methodname))
            for k, v in iattrs.items():
                setattr(runner, k, v)

            return runmethod(*(tuple(args) + tuple(nargs)), **kwargs)

        if callable(runner) and self.runner_mapper:
            if runmethod is not None:
                runner._next = continuation
            return self.runner_mapper.apply(runner)

        if runmethod is None:
            self.argparser.print_help(file=sys.stderr)
            print(file=sys.stderr)
            raise CLIUserError('Please specify a sub-command')

        return continuation()

    def get(self, key):
        return {k[1]: self.mappings[k] for k in self.mappings if k[0] == key}

    def get_list(self, key):
        keys = sorted((k for k in self.mappings.keys() if k[0] == key), key=lambda it: it[2])
        last = -1
        for k in keys:
            if k[2] - last != 1:
                return []
            last = k[2]
        return [self.mappings[k] for k in keys]

    def __str__(self):
        return type(self).__name__ + '(' + str(self.mappings) + ')'


class CLIStoreAction(argparse.Action):
    ''' Interacts with the CLIArgMapper '''

    def __init__(self, mapper, key, index=-1, mapped_name=None, *args, **kwargs):
        '''
        Parameters
        ----------
        mapper : CLIArgMapper
            CLI argument to Python mapper
        key : str
            Indicates what kind of argument is being mapped. One of `.INSTANCE_ATTRIBUTE`,
            `.METHOD_NAMED_ARG`, `.METHOD_KWARGS`, `.METHOD_NARGS`
        index : int
            Argument index. Used for maintaining the order of arguments when passed to the
            runner
        mapped_name : str
            The name to map to. optional.
        *args
            passed to `~argparse.Action`
        **kwargs
            passed to `~argparse.Action`
        '''
        super(CLIStoreAction, self).__init__(*args, **kwargs)
        if self.nargs == 0:
            raise ValueError('nargs for store actions must be > 0; if you '
                             'have nothing to store, actions such as store '
                             'true or store const may be more appropriate')
        if self.const is not None and self.nargs != argparse.OPTIONAL:
            raise ValueError('nargs must be %r to supply const' % argparse.OPTIONAL)

        self.mapper = mapper
        self.key = key
        self.name = mapped_name or self.dest
        self.index = index

    def __call__(self, parser, namespace, values, option_string=None):
        self.mapper.mappings[(self.key, self.name, self.index)] = values
        setattr(namespace, self.dest, values)


class CLIStoreTrueAction(CLIStoreAction):
    '''
    Action for storing `True` when a given option is provided
    '''
    def __init__(self, *args, **kwargs):
        '''
        Parameters
        ----------
        *args
            passed to `~.CLIStoreAction`
        **kwargs
            passed to `~.CLIStoreAction`
        '''
        super(CLIStoreTrueAction, self).__init__(*args, **kwargs)
        self.nargs = 0

    def __call__(self, parser, namespace, values, option_string=None):
        super(CLIStoreTrueAction, self).__call__(parser, namespace, True, option_string)


class CLIAppendAction(CLIStoreAction):
    '''
    Extends CLIStoreAction to append to a set of accumulated values

    Used for recording a `dict`
    '''
    def __call__(self, parser, namespace, values, option_string=None):
        '''
        Parameters
        ----------
        parser
            Ignored
        namespace : argparse.Namespace
            Namespace to add to
        values : str
            Value to add
        '''
        items = _copy.copy(_ensure_value(namespace, self.dest, []))
        items.append(values)
        self.mapper.mappings[(self.key, self.name, -1)] = items
        setattr(namespace, self.dest, items)


class CLISubCommandAction(argparse._SubParsersAction):
    '''
    Action for sub-commands

    Extends the normal action for sub-parsers to record the subparser name in a mapper
    '''

    def __init__(self, mapper, *args, **kwargs):
        '''
        Parameters
        ----------
        mapper : CLIArgMapper
            CLI argument to Python mapper
        *args
            Passed on to `argparse._SubParsersAction`
        **kwargs
            Passed on to `argparse._SubParsersAction`
        '''
        super(CLISubCommandAction, self).__init__(*args, **kwargs)
        self.mapper = mapper

    def __call__(self, *args, **kwargs):
        if self.mapper.methodname is not None:
            raise ValueError('More than one sub command has been specified!'
                             'Attempted to set {} when {} had already been'
                             ' set.'.format(self.dest, self.mapper.methodname))

        self.mapper.methodname = args[2][0]
        super(CLISubCommandAction, self).__call__(*args, **kwargs)


NOT_SET = object()


def _ensure_value(namespace, name, value):
    if getattr(namespace, name, None) is None:
        setattr(namespace, name, value)
    return getattr(namespace, name)


class CLICommandWrapper(object):
    '''
    Wraps an object such that it can be used in a command line interface
    '''

    def __init__(self, runner, mapper=None, hints=None, hints_map=None, program_name=None):
        '''
        Parameters
        ----------
        runner : object
            An object that provides the methods to be invoked
        mapper : CLIArgMapper
            Stores the arguments and associated runners for the command. A mapper is
            created if none is provided. optional
        hints : dict
            A multi-level dict describing how certain command line arguments get turned
            into attributes and method arguments. If `hints` is not provided, the hints
            are looked up by the runner's fully-qualified class name in `hints_map`. optional
        hints_map : dict
            A multi-level dict describing how certain command line arguments get turned
            into attributes and method arguments. Defaults to `CLI_HINTS <.cli_hints>`. optional
        program_name : str
            The name of the top-level program. Uses `sys.argv[0] <sys.argv>` if not provided.
            optional
        '''
        self.runner = runner
        self.mapper = CLIArgMapper() if mapper is None else mapper
        self.hints_map = hints_map or CLI_HINTS
        self.hints = self.hints_map.get(FCN(type(runner)), {}) if hints is None else hints
        self.program_name = program_name

    def extract_args(self, val):
        docstring = getattr(val, '__doc__', '')
        if not docstring:
            docstring = ''
        npdoc = npdoc_parse(docstring)
        params = npdoc.get('parameters')
        paragraphs = self._split_paras(docstring)
        if (len(paragraphs) == 1 and not params) or len(paragraphs) > 1:
            summary = paragraphs[0]
        else:
            summary = ''

        if params: # Assuming the Parameters section is the last 'paragraph'
            paragraphs = paragraphs[:-1]
        detail = '\n \n'.join(x for x in paragraphs if x)

        return summary, detail, params

    def _split_paras(self, docstring):
        paragraphs = []
        temp = ''
        for ln in docstring.split('\n'):
            ln = ln.strip()
            if ln:
                temp += '\n' + ln
            else:
                if temp:
                    paragraphs.append(temp.strip())
                temp = ''
        if temp:
            paragraphs.append(temp.strip())

        return paragraphs

    def parser(self, parser=None):
        '''
        Generates the argument parser's arguments

        Parameters
        ----------
        parser : argparse.ArgumentParser
            The parser to add the arguments to. optional: will create a parser if none is
            given
        '''
        if parser is None:
            doc = getattr(self.runner, '__doc__', None)
            if doc:
                cmd_summary, _, _ = self.extract_args(self.runner)
            else:
                cmd_summary = None
            parser = argparse.ArgumentParser(prog=self.program_name, description=cmd_summary)
        self.mapper.argparser = parser
        for key, val in vars(self.runner).items():
            if not key.startswith('_') and key not in self.hints.get('IGNORE', ()):
                parser.add_argument('--' + key, help=key.__doc__)

        _sp = [None]

        def sp():
            if _sp[0] is None:
                _sp[0] = parser.add_subparsers(dest='subparser', mapper=self.mapper,
                                               action=CLISubCommandAction)
            return _sp[0]

        runner_type_attrs = dict()
        runner_type = type(self.runner)
        for x in dir(runner_type):
            if x.startswith('_') or x in self.hints.get('IGNORE', ()):
                continue
            runner_type_attrs[x] = getattr(runner_type, x)
        if '__call__' in dir(runner_type):
            # Handle sub-commands which are, themselves, callable. Summary and details
            # must be specified on the sub-command class docstring and would have already
            # been handled by the ``isinstance(val, SubCommand)`` case below
            _, _, params = self.extract_args(runner_type.__call__)
            saved_mapper = self.mapper
            self.mapper = CLIArgMapper()
            self._handle_method(self.program_name, parser, '__call__',
                    runner_type.__call__, params)
            saved_mapper.runner_mapper = self.mapper
            self.mapper = saved_mapper
            # reset our mapper so the methods and such
        for key, val in sorted(runner_type_attrs.items()):
            if isinstance(val, (types.FunctionType, types.MethodType)):
                command_name = key.replace('_', '-')
                summary, detail, params = self.extract_args(val)
                subparser = sp().add_parser(command_name,
                        help=summary,
                        description=detail,
                        formatter_class=argparse.RawDescriptionHelpFormatter)
                self._handle_method(command_name, subparser, key, val, params)
            elif isinstance(val, property):
                doc = getattr(val, '__doc__', None)
                parser.add_argument('--' + key, help=doc,
                                    action=CLIStoreAction,
                                    key=INSTANCE_ATTRIBUTE,
                                    mapper=self.mapper)
            elif isinstance(val, SubCommand):
                summary, detail, params = self.extract_args(val)
                sub_runner = getattr(self.runner, key)
                sub_mapper = CLIArgMapper()

                self.mapper.runners[key] = _sc_runner(sub_mapper, sub_runner)

                subparser = sp().add_parser(key, help=summary, description=detail)
                type(self)(sub_runner, sub_mapper, hints_map=self.hints_map).parser(subparser)
            elif isinstance(val, IVar):
                doc = getattr(val, '__doc__', None)
                var_hints = self.hints.get(key) if self.hints else None
                if val.default_value:
                    if doc:
                        doc += '. Default is ' + repr(val.default_value)
                    else:
                        doc = 'Default is ' + repr(val.default_value)
                # NOTE: we have a default value from the val, but we don't
                # set it here -- IVars return the defaults ... by default
                arg_kwargs = dict(help=doc,
                                  action=CLIStoreAction,
                                  key=INSTANCE_ATTRIBUTE,
                                  mapper=self.mapper)
                if val.value_type == bool:
                    arg_kwargs['action'] = CLIStoreTrueAction
                names = None if var_hints is None else var_hints.get('names')
                if names is None:
                    names = ['--' + key.replace('_', '-')]
                parser.add_argument(*names, **arg_kwargs)

        return parser

    def _handle_method(self, command_name, subparser, key, val, params):
        sc_hints = self.hints.get(key) if self.hints else None
        meth = getattr(self.runner, key)
        positional_arg_count = meth.__code__.co_argcount
        self.mapper.runners[command_name] = _method_runner(self.runner, key)
        argcount = 0
        for pindex, param in enumerate(params):
            action = CLIStoreAction
            if param.val_type == 'bool':
                action = CLIStoreTrueAction

            atype = ARGUMENT_TYPES.get(param.val_type)

            arg = param.name
            arg_cli_name = arg.replace('_', '-')
            desc = param.desc
            if arg.startswith('**'):
                subparser.add_argument('--' + arg_cli_name[2:],
                                       action=CLIAppendAction,
                                       mapper=self.mapper,
                                       key=METHOD_KWARGS,
                                       type=atype,
                                       help=desc)
            elif arg.startswith('*'):
                subparser.add_argument(arg_cli_name[1:],
                                       action=action,
                                       nargs='*',
                                       key=METHOD_NARGS,
                                       mapper=self.mapper,
                                       type=atype,
                                       help=desc)
            else:
                arg_hints = self._arg_hints(sc_hints, METHOD_NAMED_ARG, arg)
                names = None if arg_hints is None else arg_hints.get('names')
                if names is None:
                    names = ['--' + arg_cli_name]
                index = pindex
                if positional_arg_count >= pindex:
                    index = -1
                argument_args = dict(action=action,
                                     key=METHOD_NAMED_ARG,
                                     mapper=self.mapper,
                                     index=index,
                                     mapped_name=arg,
                                     type=atype,
                                     help=desc)
                if arg_hints:
                    nargs = arg_hints.get('nargs')
                    if nargs is not None:
                        argument_args['nargs'] = nargs

                subparser.add_argument(*names,
                                       **argument_args)
                argcount += 1
        self.mapper.named_arg_count[key] = argcount

    def _arg_hints(self, sc_hints, atype, key):
        return None if sc_hints is None else sc_hints.get((atype, key))

    def main(self, args=None, argument_callback=None, argument_namespace_callback=None):
        '''
        Runs in a manner suitable for being the 'main' method for a command
        line interface: parses arguments (as would be done with the result of
        `parser`) from sys.argv or the provided args list and executes the
        commands specified therein

        Parameters
        ----------
        args : list
            the argument list to parse. optional
        argument_callback : callable
            a callback to add additional arguments to the command line. optional
        argument_namespace_callback : callable
            a callback to handle the parsed arguments to the command line. optional
        '''

        parser = self.parser()
        if argument_callback:
            argument_callback(parser)
        ns = parser.parse_args(args=args)
        if argument_namespace_callback:
            argument_namespace_callback(ns)
        return self.mapper.apply(self.runner)
