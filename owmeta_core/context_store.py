from itertools import chain

from rdflib.store import Store, VALID_STORE, NO_STORE
try:
    from rdflib.plugins.stores.memory import Memory
except ImportError:
    # rdflib<6.0.0
    from rdflib.plugins.memory import IOMemory as Memory

from rdflib.term import Variable, URIRef

from .context_common import CONTEXT_IMPORTS
from .rdf_utils import transitive_lookup, ContextSubsetStore


class ContextStoreException(Exception):
    pass


class ContextStore(Store):
    '''
    A store specific to a `~owmeta_core.context.Context`


    A `ContextStore` may have triples
    '''

    context_aware = True

    def __init__(self, context=None, include_stored=False, imports_graph=None, **kwargs):
        """
        Parameters
        ----------
        context : ~owmeta_core.context.Context
            The context to which this store belongs
        include_stored : bool
            If `True`, the backing store will be queried as well as the staged triples in
            `context`
        imports_graph : ~rdflib.store.Store or ~rdflib.graph.Graph
            The graph to query for imports relationships between contexts
        **kwargs
            Passed on to `Store <rdflib.store.Store.__init__>`
        """
        super(ContextStore, self).__init__(**kwargs)
        self._memory_store = None
        self._include_stored = include_stored
        self._imports_graph = imports_graph
        if context is not None:
            self._init_store(context)

    def open(self, configuration, create=False):
        if self.ctx is not None:
            return VALID_STORE
        else:
            return NO_STORE

    def _init_store(self, ctx):
        self.ctx = ctx

        if self._include_stored:
            self._store_store = RDFContextStore(ctx, imports_graph=self._imports_graph)
        else:
            self._store_store = None

        if self._memory_store is None:
            self._memory_store = Memory()
            self._init_store0(ctx)

    def _init_store0(self, ctx, seen=None):
        if seen is None:
            seen = set()
        ctxid = ctx.identifier
        if ctxid in seen:
            return
        seen.add(ctxid)
        self._memory_store.addN((s, p, o, ctxid)
                                for s, p, o
                                in ctx.contents_triples()
                                if not (isinstance(s, Variable) or
                                        isinstance(p, Variable) or
                                        isinstance(o, Variable)))
        for cctx in ctx.imports:
            self._init_store0(cctx, seen)

    def close(self, commit_pending_transaction=False):
        self.ctx = None
        self._memory_store = None

    # RDF APIs
    def add(self, triple, context, quoted=False):
        raise NotImplementedError("This is a query-only store")

    def addN(self, quads):
        raise NotImplementedError("This is a query-only store")

    def remove(self, triple, context=None):
        raise NotImplementedError("This is a query-only store")

    def triples(self, triple_pattern, context=None):
        if self._memory_store is None:
            raise ContextStoreException("Database has not been opened")
        context = getattr(context, 'identifier', context)
        context_triples = []
        if self._store_store is not None:
            context_triples.append(self._store_store.triples(triple_pattern,
                                                             context))
        return chain(self._memory_store.triples(triple_pattern, context),
                     *context_triples)

    def __len__(self, context=None):
        """
        Number of statements in the store. This should only account for non-
        quoted (asserted) statements if the context is not specified,
        otherwise it should return the number of statements in the formula or
        context given.

        :param context: a graph instance to query or None

        """
        if self._memory_store is None:
            raise ContextStoreException("Database has not been opened")
        if self._store_store is None:
            return len(self._memory_store)
        else:
            # We don't know which triples may overlap, so we can't return an accurate count without doing something
            # expensive, so we just give up
            raise NotImplementedError()

    def contexts(self, triple=None):
        """
        Generator over all contexts in the graph. If triple is specified,
        a generator over all contexts the triple is in.

        if store is graph_aware, may also return empty contexts

        :returns: a generator over Nodes
        """
        if self._memory_store is None:
            raise ContextStoreException("Database has not been opened")
        seen = set()
        rest = ()

        if self._store_store is not None:
            rest = self._store_store.contexts(triple)

        for ctx in chain(self._memory_store.contexts(triple), rest):
            if ctx in seen:
                continue
            seen.add(ctx)
            yield ctx


class RDFContextStore(ContextSubsetStore):
    # Returns triples imported by the given context
    context_aware = True

    def __init__(self, context=None, imports_graph=None, include_imports=True, **kwargs):
        store = context.rdf.store
        super(RDFContextStore, self).__init__(store=store, **kwargs)
        self.__imports_graph = imports_graph
        self.__store = store
        self.__context = context
        self.__include_imports = include_imports

    def init_contexts(self):
        if self.__store is not None:
            if not self.__context or self.__context.identifier is None:
                return {getattr(x, 'identifier', x)
                        for x in self.__store.contexts()}
            elif self.__include_imports:
                context = None
                if self.__imports_graph is not None:
                    if isinstance(self.__imports_graph, URIRef):
                        query_graph = self.__store
                        context = self.__imports_graph
                    else:
                        query_graph = self.__imports_graph
                else:
                    query_graph = self.__store

                return transitive_lookup(
                        query_graph,
                        self.__context.identifier,
                        CONTEXT_IMPORTS,
                        context)
            else:
                # XXX we should maybe check that the provided context actually exists in
                # the backing graph -- at this point, it's more-or-less assumed in this
                # case though if self.__include_imports is True, we could have an empty
                # set of imports => we query against everything
                return set([self.__context.identifier])
