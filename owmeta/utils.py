"""
Common utilities for translation, massaging data, etc., that don't fit
elsewhere in owmeta
"""
import re

__all__ = ['grouper']


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
