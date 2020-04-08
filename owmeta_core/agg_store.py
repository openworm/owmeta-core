from rdflib import plugin
from rdflib.store import Store, NO_STORE

from .utils import FCN


class AggregateStore(Store):
    '''
    A read-only aggregate of RDFLib `stores <rdflib.store.Store>`
    '''
    context_aware = True
    formula_aware = True
    graph_aware = True
    transaction_aware = True
    supports_range_queries = True

    def __init__(self, configuration=None, identifier=None):
        super(AggregateStore, self).__init__(configuration, identifier)
        self.__stores = []

    def open(self, configuration, create=True):
        if not isinstance(configuration, (tuple, list)):
            return NO_STORE
        self.__stores = []
        for store_key, store_conf in configuration:
            store = plugin.get(store_key, Store)()
            store.open(store_conf)
            self.__stores.append(store)

    def triples(self, triple, context=None):
        for store in self.__stores:
            for trip in store.triples(triple, context=context):
                yield trip

    def triples_choices(self, triple, context=None):
        for store in self.__stores:
            for trip in store.triples_choices(triple, context=context):
                yield trip

    def __len__(self):
        # rdflib specifies a context argument for __len__, but how do you even pass that
        # argument to len?
        return sum(len(store) for store in self.__stores)

    def contexts(self, triple=None):
        for store in self.__stores:
            for ctx in store.contexts(triple):
                yield ctx

    def prefix(self, namespace):
        prefix = None
        for store in self.__stores:
            aprefix = store.prefix(namespace)
            if aprefix and prefix and aprefix != prefix:
                msg = 'multiple prefixes ({},{}) for namespace {}'.format(prefix, aprefix, namespace)
                raise AggregatedStoresConflict(msg)
            prefix = aprefix
        return prefix

    def namespace(self, prefix):
        namespace = None
        for store in self.__stores:
            anamespace = store.namespace(prefix)
            if anamespace and namespace and anamespace != namespace:
                msg = 'multiple namespaces ({},{}) for prefix {}'.format(namespace, anamespace, prefix)
                raise AggregatedStoresConflict(msg)
            namespace = anamespace
        return namespace

    def namespaces(self):
        for store in self.__stores:
            for ns in store.namespaces():
                yield ns

    def close(self, *args, **kwargs):
        for store in self.__stores:
            store.close(*args, **kwargs)

    def gc(self):
        for store in self.__stores:
            store.gc()

    def add(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def addN(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def remove(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def add_graph(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def remove_graph(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def create(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def destroy(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def commit(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def rollback(self, *args, **kwargs):
        raise UnsupportedAggregateOperation()

    def __repr__(self):
        return '%s(%s)' % (FCN(type(self)), ', '.join(repr(s) for s in self.__stores))


class UnsupportedAggregateOperation(Exception):
    pass


class AggregatedStoresConflict(Exception):
    pass
