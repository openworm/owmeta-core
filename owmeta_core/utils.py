# -*- coding: utf-8 -*-
"""
Common utilities for translation, massaging data, etc., that don't fit
elsewhere in owmeta_core
"""
import functools
import importlib
import re

__all__ = ['grouper', 'slice_dict']

PROVIDER_PATH_FORMAT = r'''
(?P<module>(?:\w+)(?:\.\w+)*)
:
(?P<provider>(?:\w+)(?:\.\w+)*)'''

PROVIDER_PATH_RE = re.compile(PROVIDER_PATH_FORMAT, flags=re.VERBOSE)


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    while True:
        l = []
        try:
            for x in args:
                l.append(next(x))
        except Exception:
            pass
        yield l
        if len(l) < n:
            break


def slice_dict(d, s):
    return {k: v for k, v in d.items() if k in s}


def FCN(cls):
    return str(cls.__module__) + '.' + str(cls.__name__)


def aslist(fun):
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
        return list(fun(*args, **kwargs))
    return wrapper


UNSET = object()


def getattrs(obj, names, default=UNSET):
    p = obj
    try:
        for x in names:
            p = getattr(p, x)
        return p
    except AttributeError:
        if default is UNSET:
            raise
        return default


def retrieve_provider(provider_path):
    '''
    Look up a "provider" specified by a string.

    Path to an object that provides something. The format is similar to that for
    setuptools entry points: ``path.to.module:path.to.provider.callable``. Notably,
    there's no name and "extras" are not supported.

    Parameters
    ----------
    provider_path : str
        The path to the provider

    Returns
    -------
    object
        The provider

    Raises
    ------
    ValueError
        The `provider_path` format doesn't match the expected pattern
    AttributeError
        Some element in the path is missing
    '''
    md = PROVIDER_PATH_RE.match(provider_path)
    if not md:
        raise ValueError('Format of the provider path is incorrect')
    module = md.group('module')
    provider = md.group('provider')
    m = importlib.import_module(module)
    attr_chain = provider.split('.')
    return getattrs(m, attr_chain)


def ellipsize(s, max_length):
    t = s[:max_length]
    if t != s:
        if len(t) <= 1:
            return t
        return t[:-1] + '…'
    return t
