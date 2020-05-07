from json import dumps
from weakref import WeakValueDictionary
import logging

from rdflib import plugin
from rdflib.store import Store, VALID_STORE

from .utils import FCN


L = logging.getLogger(__name__)

RDFLIB_PLUGIN_KEY = 'owmeta_core_bds'


class BundleDependencyStore(Store):
    '''
    A read-only RDFLib `~rdflib.store.Store` that supports the extra stuff we need from
    dependencies
    '''

    context_aware = True
    '''
    Specified by RDFLib. Required to be True for `~rdflib.graph.ConjunctiveGraph` stores.

    Wrapped store MUST be context-aware. This is enforced by :meth:`open`.
    '''

    def __init__(self, wrapped=None, excludes=()):
        self.wrapped = wrapped
        self.excludes = set(excludes)

    def open(self, configuration):
        '''
        Creates and opens the configured store.

        Also verifies that the provided store is context-aware
        '''
        if isinstance(configuration, (list, tuple)):
            try:
                store_key, store_conf = configuration
            except ValueError:
                raise ValueError('Configuration should be in the form (store_type,'
                        ' store_configuration)')
        elif isinstance(configuration, dict):
            try:
                store_key = configuration['type']
                store_conf = configuration['conf']
            except KeyError:
                raise ValueError('Missing type and conf entries')
            excludes = configuration.get('excludes', ())
        else:
            raise ValueError('Invalid configuration for ' + RDFLIB_PLUGIN_KEY)

        if _is_cacheable(RDFLIB_PLUGIN_KEY, configuration):
            bds_ck = _cache_key(RDFLIB_PLUGIN_KEY, configuration)
            bds_cached_store = _store_cache.get(bds_ck, None)
            if bds_cached_store is not None:
                if bds_cached_store is not self:
                    self.wrapped = bds_cached_store
                # We've already opened the primary store for this config and put it in the
                # cache, so nothing left to do...
                return VALID_STORE

        if _is_cacheable(store_key, store_conf):
            ck = _cache_key(store_key, store_conf)
            cached_store = _store_cache.get(ck, None)
            if cached_store is None:
                cached_store = plugin.get(store_key, Store)()
                cached_store.open(store_conf)
                _store_cache[ck] = cached_store
            self.wrapped = cached_store
        else:
            self.wrapped = plugin.get(store_key, Store)()
            self.wrapped.open(store_conf)

        assert self.wrapped.context_aware, 'Wrapped store must be context-aware.'
        self.supports_range_queries = getattr(self.wrapped, 'supports_range_queries',
                False)
        if _is_cacheable(RDFLIB_PLUGIN_KEY, configuration):
            # If there were a stored cacheable configuration with the same cache key as
            # ours, we would have already set it as our wrapped (or found we are the
            # primary for that configuration) and returned, so we can safely set ourselves
            # in the cache
            _store_cache[bds_ck] = self
        return VALID_STORE

    def triples(self, pattern, context=None):
        ctxid = getattr(context, 'identifier', context)
        if ctxid in self.excludes:
            return
        for triple, contexts in self.wrapped.triples(pattern, context=context):
            filtered_contexts = self._contexts_filter(contexts)
            has_valid_contexts = next(filtered_contexts)
            if has_valid_contexts:
                yield triple, filtered_contexts

    def triples_choices(self, pattern, context=None):
        ctxid = getattr(context, 'identifier', context)
        if ctxid in self.excludes:
            return
        for triple, contexts in self.wrapped.triples_choices(pattern, context=context):
            filtered_contexts = self._contexts_filter(contexts)
            has_valid_contexts = next(filtered_contexts)
            if has_valid_contexts:
                yield triple, filtered_contexts

    def __len__(self, context=None):
        ctxid = getattr(context, 'identifier', context)
        if ctxid in self.excludes:
            return 0
        return sum(1 for _ in self.triples((None, None, None), context=context))

    def contexts(self, triple=None):
        cgen = self._contexts_filter(self.wrapped.contexts(triple))
        next(cgen)
        for c in cgen:
            yield c

    def _contexts_filter(self, contexts):
        contexts_iter = iter(contexts)
        excludes = self.excludes
        for c in contexts_iter:
            ctxid = getattr(c, 'identifier', c)
            if ctxid not in excludes:
                yield True
                yield c
                break
        else:  # no break
            yield False
            return
        for c in contexts_iter:
            ctxid = getattr(c, 'identifier', c)
            if ctxid not in excludes:
                yield c

    def prefix(self, namespace):
        return self.wrapped.prefix(namespace)

    def namespace(self, prefix):
        return self.wrapped.namespace(prefix)

    def gc(self):
        self.wrapped.gc()

    def add(self, *args, **kwargs):
        raise NotImplementedError()

    def addN(self, *args, **kwargs):
        raise NotImplementedError()

    def remove(self, *args, **kwargs):
        raise NotImplementedError()

    def add_graph(self, *args, **kwargs):
        raise NotImplementedError()

    def remove_graph(self, *args, **kwargs):
        raise NotImplementedError()

    def create(self, *args, **kwargs):
        raise NotImplementedError()

    def destroy(self, *args, **kwargs):
        raise NotImplementedError()

    def commit(self, *args, **kwargs):
        raise NotImplementedError()

    def rollback(self, *args, **kwargs):
        raise NotImplementedError()

    def __repr__(self):
        return '%s(%s)' % (FCN(type(self)), repr(self.wrapped))


def _is_cacheable(store_key, store_conf):
    '''
    Determine whether the store store's key and configuration indicate that the
    corresponding store can be reused by another `BundleDependencyStore` rather than being
    created anew.

    Parameters
    ----------
    store_key : str
        The key by which the store type can be looked up as an RDFLib store plugin
    store_conf : object
        Configuration parameters, such as would be passed to `Store.open`, which may
        indicate whether the store can be cached

    Returns
    -------
    bool
        True If the given store configuration is cacheable
    '''
    if store_key == 'agg':
        return all(_is_cacheable(k, c) for k, c in store_conf)
    if store_key == 'FileStorageZODB':
        if isinstance(store_conf, dict) and store_conf.get('read_only', False):
            return True
    if store_key == RDFLIB_PLUGIN_KEY:
        if isinstance(store_conf, (list, tuple)):
            try:
                kind, conf = store_conf
            except ValueError:
                L.warning('Inappropriate configuration for ' + RDFLIB_PLUGIN_KEY)
                return False
        elif isinstance(store_conf, dict):
            try:
                kind = store_conf['type']
                conf = store_conf['conf']
            except KeyError:
                L.warning('Inappropriate configuration for ' + RDFLIB_PLUGIN_KEY)
                return False
        else:
            L.warning('Unknown type of configuration for ' + RDFLIB_PLUGIN_KEY)
            return False
        return _is_cacheable(kind, conf)
    return False


def _cache_key(store_key, store_conf):
    '''
    Produces a key for use in the store cache.

    Parameters
    ----------
    store_key : str
        The key for the type of store which would be cached
    store_conf : object
        The configuration parameters for the store which would be cached
    '''
    return dumps([store_key, store_conf],
            separators=(',', ':'),
            sort_keys=True)


_store_cache = WeakValueDictionary()
'''
Cache of stores previously cached by a `BDS <BundleDependencyStore>`.

We don't want to keep hold of the store if there's no BDS using it, so we only reference
the store weakly.
'''
