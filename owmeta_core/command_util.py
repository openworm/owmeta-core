'''
Utilities for making objects that work with the `CLICommandWrapper`
'''
from .utils import FCN

DEFAULT_OWM_DIR = '.owm'


class IVar(object):
    '''
    A descriptor for instance variables amended to provide some attributes like
    default values, value types, etc.
    '''

    _instance_counter = 0

    def __init__(self, default_value=None, doc=None, value_type=str, name=None):
        self.name = ('_ivar_' + str(IVar._instance_counter)) if name is None else name
        IVar._instance_counter += 1
        self.default_value = default_value
        self.__doc__ = doc.strip() if doc is not None else doc
        self.value_type = value_type

    def __repr__(self):
        return '{}(name={}, doc={}, value_type={}, default_value={})'.format(
                FCN(type(self)), repr(self.name), repr(self.__doc__), repr(self.value_type), repr(self.default_value))

    __str__ = __repr__

    def __get__(self, target, typ=None):
        if target is None:
            return self
        return getattr(target, self.name, self.default_value)

    def __set__(self, target, value):
        setattr(target, self.name, value)

    @classmethod
    def property(cls, wrapped=None, *args, **kwargs):
        '''
        Creates a `PropertyIVar` from a method

        Typically, this will be used as a decorator for a method

        Parameters
        ----------
        wrapped : types.FunctionType or types.MethodType
            The function to wrap. optional: if omitted, returns a function that can be
            invoked later to create the `PropertyIVar`
        '''
        if wrapped is not None and \
                callable(wrapped) and \
                len(args) == 0 and \
                len(kwargs) == 0: # we only got the wrapped. Just execute now.
            return _property_update(wrapped)

        first_arg = wrapped
        aargs = list(args)

        def wrapper(wrapped):
            if first_arg is not None:
                args = [first_arg] + list(aargs)
            else:
                args = aargs
            return _property_update(wrapped, *args, **kwargs)
        return wrapper


def _property_update(wrapped, *args, **kwargs):
    res = PropertyIVar(*args, **kwargs)
    res.value_getter = wrapped
    wrapped_doc = getattr(wrapped, '__doc__', None)
    if wrapped_doc:
        res.__doc__ = wrapped_doc
    res.__doc__ = '' if res.__doc__ is None else res.__doc__.strip()
    return res


class SubCommand(object):
    '''
    A descriptor that wraps objects which function as sub-commands to
    `~owmeta_core.command.OWM` or to other sub-commands
    '''

    def __init__(self, cmd):
        self.cmd = cmd
        self.__doc__ = getattr(cmd, '__doc__', '')

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.cmd))

    __str__ = __repr__

    def __get__(self, target, typ=None):
        if target is None:
            return self
        return self.cmd(target)


class PropertyIVar(IVar):
    '''
    An `.IVar` that functions similarly to a `property`


    Typically a PropertyIVar will be created by using `.IVar.property` as a decorator for
    a method like::

        class A(object):

            @IVar.property('default_value')
            def prop(self):
                return 'value'

    .. automethod:: __get__
    '''
    def __init__(self, *args, **kwargs):
        super(PropertyIVar, self).__init__(*args, **kwargs)
        self.value_getter = None
        self.value_setter = None
        self._setter_called_flag = '_' + self.name + '_is_set'

    def setter(self, fset):
        '''
        Decorator for the setter that goes along with this property.

        See Also
        --------
        property
        '''
        res = type(self)(default_value=self.default_value,
                         doc=self.__doc__,
                         value_type=self.value_type,
                         name=self.name)
        res.value_getter = self.value_getter
        res.value_setter = fset
        return res

    def __set__(self, target, value):
        if self.value_setter is None:
            raise AttributeError("can't set attribute")
        setattr(target, self._setter_called_flag, True)
        self.value_setter(target, value)

    def __get__(self, target, objecttype=None):
        ''' Executes the provided getter

        When the getter is first called, and when a setter is also defined, the setter will be called with the default
        value before the getter is called for the first time. _Even if the default_value is not set explicitly, the
        setter will still be called with 'None'.
        '''
        if target is None:
            return self

        if self.value_setter is not None and \
                not hasattr(target, self._setter_called_flag):
            setattr(target, self._setter_called_flag, True)
            self.value_setter(target, self.default_value)
        return self.value_getter(target)


class GeneratorWithData(object):
    def __init__(self, generator, header=None, text_format=None, default_columns=None, columns=None):
        self._gen = iter(generator)
        self.header = header
        if columns is tuple:
            if not self.header:
                raise Exception('Must provide a header if columns is `tuple`')
            columns = []
            for i, _ in enumerate(self.header):
                columns.append((lambda i: lambda x: x[i])(i))
            self.columns = columns
        else:
            self.columns = columns
        self.default_columns = default_columns
        self.text_format = text_format or None

    def __iter__(self):
        for m in self._gen:
            yield m

    def __next__(self):
        return next(self._gen)

    next = __next__


class GenericUserError(Exception):
    '''
    An error which should be reported to the user. Not necessarily an error that is
    the user's fault
    '''
