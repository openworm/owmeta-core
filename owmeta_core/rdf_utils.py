from __future__ import print_function

from rdflib.graph import Graph
from rdflib.term import Literal, URIRef
from rdflib.store import Store

# Directions for traversal across triples
UP = 'up'
''' Object to Subject direction for traversal across triples. '''

DOWN = 'down'
''' Subject to Object direction for traversal across triples. '''


def print_graph(g, hide_namespaces=False):
    s = g.serialize(format='n3').decode("UTF-8")
    if hide_namespaces:
        lines = s.splitlines()
        s = "\n".join(l for l in lines if not l.startswith("@prefix"))
    print(s)


def serialize_rdflib_term(x, namespace_manager=None):
    return x.n3(namespace_manager)


def deserialize_rdflib_term(x):
    if isinstance(x, Literal):
        x = x.toPython()
        if isinstance(x, Literal):
            x = str(x)
    return x


def triple_to_n3(trip, namespace_manager=None):
    p = ''
    ns = set([])
    for x in trip:
        s = serialize_rdflib_term(x, namespace_manager)
        if isinstance(x, URIRef) and s[0] != '<':
            ns.add(s.split(':', 1)[0])
        elif isinstance(x, Literal) and '^^' in s and s[-1] != '>':
            ns.add(s.split('^^', 1)[1].split(':', 1)[0])

        p += s + ' '
    return p


def triples_to_bgp(trips, namespace_manager=None, show_namespaces=False):
    # XXX: Collisions could result between the variable names of different
    # objects
    g = ""
    ns = set([])
    for y in trips:
        g += triple_to_n3(y) + ".\n"

    if (namespace_manager is not None) and show_namespaces:
        g = "".join('@prefix ' + str(x) + ': ' + y.n3() + ' .\n'
                    for x, y
                    in namespace_manager.namespaces()
                    if x in ns) + g

    return g


_none_singleton_set = frozenset([None])


def transitive_lookup(graph, start, predicate, context=None, direction=DOWN, seen=None):
    '''
    Do a transitive lookup over an `rdflib.graph.Graph` or `rdflib.store.Store`

    In other words, finds all resources which relate to `start` through zero or more
    `predicate` relationships. `start` itself will be included in the return value.

    Loops in the input `graph` will not cause non-termination.

    Parameters
    ----------
    graph : rdflib.graph.Graph or rdflib.store.Store
        The graph to query
    start : rdflib.term.Identifier
        The resource in the graph to start from
    predicate : rdflib.term.URIRef
        The predicate relating terms in the closure
    context : rdflib.graph.Graph or rdflib.term.URIRef
        The context in which the query should run. Optional
    direction : DOWN or UP
        The direction in which to traverse
    seen : set of rdflib.term.Identifier
        A set of terms which have already been "seen" by the algorithm. Useful for
        repeated calls to `transitive_lookup`. Note: if the `start` is in `seen`, queries
        from `start` will still be done, but any items in the result of *those* queries
        will not be queried for if in `seen`. Optional

    Returns
    -------
    set of rdflib.term.Identifier
        resources in the transitive closure of `predicate` from `start`
    '''
    if seen:
        res = seen
    else:
        res = set()
    border = set([start])
    while border:
        new_border = set()
        if direction is DOWN:
            qx = (list(border), predicate, None)
            idx = 2
        else:
            qx = (None, predicate, list(border))
            idx = 0
        for t in graph.triples_choices(qx, context=context):
            if isinstance(t[0], tuple):
                o = t[0][idx]
            else:
                o = t[idx]
            if o not in res:
                new_border.add(o)
        res |= border
        border = new_border
    res -= _none_singleton_set
    return res


class BatchAddGraph(object):
    ''' Wrapper around graph that turns calls to 'add' into calls to 'addN' '''
    def __init__(self, graph, batchsize=1000, _parent=None, *args, **kwargs):
        self.graph = graph
        self.g = (graph,)
        if _parent:
            self.batch = _parent.batch
            self.batchsize = _parent.batchsize
            self._parent = _parent
        else:
            self.batchsize = batchsize
            self._parent = None
            self.reset()

    def reset(self):
        self.batch = []
        self._count = 0

    @property
    def count(self):
        if self._parent:
            return self._parent.count
        else:
            return self._count

    @count.setter
    def count(self, value):
        if self._parent:
            self._parent.count = value
        else:
            self._count = value

    def add(self, triple):
        if self.count > 0 and self.count % self.batchsize == 0:
            self.graph.addN(self.batch)
            self.batch = []
        self.count += 1
        self.batch.append(triple + self.g)

    def get_context(self, ctx):
        return BatchAddGraph(self.graph.get_context(ctx), _parent=self)

    def __enter__(self):
        self.reset()
        return self

    def __exit__(self, *exc):
        if exc[0] is None:
            self.graph.addN(self.batch)


transitive_subjects = transitive_lookup
''' Alias to `transitive_lookup` '''


class ContextSubsetStore(Store):
    # Returns triples imported by the given context
    context_aware = True

    def __init__(self, store, **kwargs):
        super(ContextSubsetStore, self).__init__(**kwargs)
        self.__store = store
        self.__context_ids = None
        self.__query_perctx = None

    def init_contexts(self):
        raise NotImplementedError

    def __init_contexts(self):
        if self.__context_ids is None:
            self.__context_ids = self.init_contexts()

        if self.__store is not None and self.__query_perctx is None:
            total_triples = self.__store.__len__()
            per_ctx_triples = sum(self.__store.__len__(context=ctx)
                    for ctx in self.__context_ids)

            self.__query_perctx = total_triples > per_ctx_triples

    def triples(self, pattern, context=None):
        self.__init_contexts()

        ctx = self._determine_context(context)
        if ctx is _BAD_CONTEXT:
            return

        # If the sum of lengths of the selected contexts is less than total number of
        # triples, query each context in series
        if pattern == (None, None, None) and ctx is None and self.__query_perctx:
            imports = self.__context_ids
            store = self.__store
            for ctx0 in imports:
                for t, tctxs in store.triples(pattern, ctx0):
                    contexts = set(getattr(c, 'identifier', c) for c in tctxs)
                    yield t, imports & contexts
        else:
            for t in self.__store.triples(pattern, ctx):
                contexts = set(getattr(c, 'identifier', c) for c in t[1])
                if self.__context_ids:
                    inter = self.__context_ids & contexts
                else:
                    inter = contexts
                if inter:
                    yield t[0], inter

    def remove(self, pattern, context=None):
        self.__init_contexts()

        ctx = self._determine_context(context)
        if ctx is _BAD_CONTEXT:
            return
        for t in self.__store.triples(pattern, ctx):
            triple = t[0]
            contexts = set(getattr(c, 'identifier', c) for c in t[1])
            if self.__context_ids:
                inter = self.__context_ids & contexts
            else:
                inter = contexts
            for ctx in inter:
                self.__store.remove((triple[0], triple[1], triple[2]), ctx)

    def triples_choices(self, pattern, context=None):
        self.__init_contexts()

        ctx = self._determine_context(context)
        if ctx is _BAD_CONTEXT:
            return

        for t in self.__store.triples_choices(pattern, ctx):
            contexts = set(getattr(c, 'identifier', c) for c in t[1])
            if self.__context_ids:
                inter = self.__context_ids & contexts
            else:
                inter = contexts

            if inter:
                yield t[0], inter

    def _determine_context(self, context):
        # This is a method that has to contend with RDFLib's abiding confusion over
        # whether Store's should return Graphs. This is stupid, because of course they
        # shouldn't, but RDFLib acts like they should...and so here we are
        context_id = getattr(context, 'identifier', context)
        if context_id is not None and context_id not in self.__context_ids:
            return _BAD_CONTEXT
        if len(self.__context_ids) == 1 and context_id is None:
            # Micro-benchmarked this with timeit: it's faster than tuple(s)[0] and
            # next(iter(s),None)
            for context_id in self.__context_ids:
                break
        if context_id is None:
            return None
        # We shouldn't be querying from this store, but we pass in ourselves as the store
        # to prevent RDFLib from making a new memory story
        return Graph(identifier=context_id, store=self)

    def contexts(self, triple=None):
        if triple is not None:
            for x in self.triples(triple):
                for c in x[1]:
                    yield getattr(c, 'identifier', c)
        else:
            self.__init_contexts()
            for c in self.__context_ids:
                yield c

    def namespace(self, prefix):
        return self.__store.namespace(prefix)

    def prefix(self, uri):
        return self.__store.prefix(uri)

    def bind(self, prefix, namespace):
        return self.__store.bind(prefix, namespace)

    def namespaces(self):
        for x in self.__store.namespaces():
            yield x

    def __str__(self):
        return f'{type(self).__name__}(store={self.__store})'


_BAD_CONTEXT = object()
