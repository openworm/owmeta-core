"""
Common utilities for translation, massaging data, etc., that don't fit
elsewhere in owmeta_core
"""
import re

__all__ = ['grouper', 'slice_dict']


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
