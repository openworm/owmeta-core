from rdflib import plugin
from rdflib.store import Store

from .utils import FCN


class BundleDependencyStore(Store):
    '''
    A read-only RDFLib `~rdflib.store.Store` that supports the extra stuff we need from
    dependencies
    '''
    def __init__(self, wrapped=None, excludes=()):
        self.wrapped = wrapped
        self.excludes = set(excludes)

    def open(self, configuration):
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
            return NO_STORE
        self.wrapped = plugin.get(store_key, Store)()
        self.wrapped.open(store_conf)

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
