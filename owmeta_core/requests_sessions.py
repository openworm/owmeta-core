'''
A collection of functions that produce `requests.Session` objects.

A few methods request a "session provider". The functions in here are providers of that kind
'''

import requests
from cachecontrol import CacheControl
from cachecontrol.caches.file_cache import FileCache
from cachecontrol.heuristics import ExpiresAfter


def caching():
    '''
    Provides a `requests.Session` that puts cached responses in :file:`.owmeta_http_cache`

    In absence of explict cache-control headers, uses a heuristic of caching cacheable
    responses for up to a day.
    '''
    http_cache_directory = '.owmeta_http_cache'
    base_session = requests.Session()
    http_cache = FileCache(http_cache_directory)
    return CacheControl(base_session, cache=http_cache, heuristic=ExpiresAfter(days=1))
