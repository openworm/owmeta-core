from __future__ import print_function
import warnings
import logging
from itertools import chain

import six

from .utils import FCN
from .ranged_objects import InRange
from .rdf_utils import transitive_subjects, UP, DOWN

L = logging.getLogger(__name__)

__all__ = [
    "GraphObject",
    "GraphObjectQuerier",
    "GraphObjectChecker",
    "ComponentTripler",
    "IdentifierMissingException",
    "ZeroOrMoreTQLayer",
]

EMPTY_SET = frozenset([])


class Variable(int):
    """
    A marker used in `GraphObjectQuerier` for variables in a query
    """


class _Range(InRange):
    pass


class GraphObject(object):
    """
    An object which can be included in the object graph.

    An abstract base class.
    """

    def __init__(self, **kwargs):
        super(GraphObject, self).__init__(**kwargs)
        self.properties = []
        self.owner_properties = []

    @property
    def identifier(self):
        """ Must return an object representing this object or else
        raise an Exception. """
        raise NotImplementedError()

    @property
    def defined(self):
        """ Returns true if an :meth:`identifier` would return an identifier
        """
        raise NotImplementedError()

    def variable(self):
        """ Must return a `~owmeta_core.graph_object.Variable` object that identifies this
        `.GraphObject` in queries.

        The variable can be randomly generated when the object is created and
        stored in the object.
        """
        raise NotImplementedError()

    @property
    def idl(self):
        if self.defined:
            return self.identifier
        else:
            return self.variable()

    def __hash__(self):
        raise NotImplementedError()

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif isinstance(other, GraphObject):
            return self.idl == other.idl

    def __lt__(self, other):
        if isinstance(other, GraphObject):
            return self.idl < other.idl
        else:
            return id(self) < id(other)


class GraphObjectChecker(object):
    '''
    Checks the graph of defined GraphObjects for
    '''

    def __init__(self, query_object, graph, parallel=False, sort_first=False):
        self.query_object = query_object
        self.graph = graph

    def __call__(self):
        tripler = ComponentTripler(self.query_object)
        L.debug('GOC: Checking {}'.format(self.query_object))
        for x in sorted(tripler()):
            if x not in self.graph:
                L.debug('GOC: Failed on {}'.format(x))
                return False
        return True


class GraphObjectValidator(object):
    def __init__(self, query_object, graph, parallel=False):
        self.query_object = query_object
        self.graph = graph

    def __call__(self):
        return True


class GraphObjectQuerier(object):

    """ Performs queries for objects in the given graph.

    The querier queries for objects at the center of a star graph. In SPARQL,
    the query has the form::

        SELECT ?x WHERE {
            ?x  <p1> ?o1 .
            ?o1 <p2> ?o2 .
             ...
            ?on <pn> <a> .

            ?x  <q1> ?n1 .
            ?n1 <q2> ?n2 .
             ...
            ?nn <qn> <b> .
        }

    It is allowed that ``<px> == <py>`` for ``x != y``.

    Queries such as::

        SELECT ?x WHERE {
            ?x  <p1> ?o1 .
             ...
            ?on <pn>  ?y .
        }

    or::

        SELECT ?x WHERE {
            ?x  <p1> ?o1 .
             ...
            ?on <pn>  ?x .
        }

    or::

        SELECT ?x WHERE {
            ?x  ?z ?o .
        }

    or::

        SELECT ?x WHERE {
            ?x  ?z <a> .
        }

    are not supported and will be ignored without error.
    """

    def __init__(self, q, graph, parallel=False, hop_scorer=None):
        """
        Call the GraphObjectQuerier object to perform the query.

        Parameters
        ----------
        q : :class:`GraphObject`
            The object which is queried on
        graph : :class:`object`
            The graph from which the objects are queried. Must implement a
            method :meth:`triples` that takes a triple pattern, ``t``, and
            returns a set of triples matching that pattern. The pattern for
            ``t`` is ``t[i] = None``, 0 <= i <= 2, indicates that the i'th
            position can take any value.

            The ``graph`` method can optionally implement the 'range query' 'interface':
            the graph must have a property ``supports_range_queries`` equal to `True` and
            :meth:`triples` must accept an `~owmeta_core.ranged_objects.InRange` object in
            the object position of the query triple, but only for literals
        hop_scorer : callable
            Returns a score for a hop (a four-tuple, (subject, predicate,
            object, target)) indicating how selective the query would be for
            that hop, with lower numbers being more selective. In general the
            score should only take the given hop into account -- it should not
            take previously given hops into account when calculating a score.
        """

        self.query_object = q
        L.debug('GOQ graph %s', graph)
        self.graph = _default_tq_layers(graph)
        if parallel:
            warnings.warn('Parallel execution is not supported')
        self.results = dict()
        self.triples_cache = dict()
        self.hop_scorer = hop_scorer

    def do_query(self):
        L.debug('do_query: Graph {}'.format(self.graph))
        if self.query_object.defined:
            L.debug('do_query: Query object {} is already defined'.format(self.query_object))
            gv = GraphObjectChecker(self.query_object, self.graph)
            if gv():
                return set([self.query_object.identifier])
            else:
                L.debug('do_query: Query graph does not align with the backing graph')
                return EMPTY_SET

        qp = _QueryPreparer(self.query_object)
        paths = qp()
        if len(paths) == 0:
            return EMPTY_SET
        h = self.merge_paths(paths)
        L.debug('do_query: merge_paths_result:\n{}'.format(self._format_merged(h)))
        return self.query_path_resolver(h)

    def merge_paths(self, l):
        """ Combines a list of lists into a multi-level table with
        the elements of the lists as the keys. For given::

            [[a, b, c], [a, b, d], [a, e, d]]

        merge_paths returns::

            {a: {b: {c: {},
                     d: {}},
                 e: {d: {}}}}
        """
        res = dict()
        L.debug("merge_paths: path {}".format(_format_paths(l)))
        for x in l:
            if len(x) > 0:
                tmp = res.get(x[0], [])
                tmp.append(x[1:])
                res[x[0]] = tmp

        for x in res:
            res[x] = self.merge_paths(res[x])

        return res

    def _format_merged(self, merge, depth=0):
        sio = six.StringIO()
        for triple, remainder in merge.items():
            idx = triple.index(None)
            other_idx = 0 if (idx == 2) else 2
            print((depth * 4 * ' ') + str(triple[1]) + '->' + str(triple[other_idx]), file=sio)
            print(self._format_merged(remainder, depth+1), file=sio, end='')
        return sio.getvalue()

    def query_path_resolver(self, path_table):
        join_args = []
        goal = None
        for hop in sorted(path_table.keys(), key=self.score):
            L.debug("HOP %s", str(hop))
            goal = hop[3]
            self._qpr_helper(path_table[hop], hop, join_args)
        if len(join_args) == 1:
            return join_args[0]
        elif len(join_args) > 0:
            L.debug("Joining {} args on {}".format(len(join_args), goal))
            join_args = sorted(join_args, key=len)
            res = join_args[0]
            res.intersection_update(*join_args[1:])
            L.debug("Joined {}(sizes={}) args on {}. Result size = {}".format(len(join_args),
                [len(s) for s in join_args], goal, len(res)))
            return res
        else:
            return EMPTY_SET

    def _qpr_helper(self, sub, search_triple, join_args):
        seen = set()
        try:
            idx = search_triple.index(None)
            other_idx = 0 if (idx == 2) else 2
            qx = None

            if isinstance(search_triple[other_idx], Variable):
                sub_results = list(self.query_path_resolver(sub))

                if idx == 2:
                    qx = (sub_results, search_triple[1], None)
                else:
                    qx = (None, search_triple[1], sub_results)

                if sub_results:
                    trips = self.triples_choices(qx)
                else:
                    trips = iter(())
            else:
                # join_args is assumed to be sorted such that it the most selective query was executed first, so we
                # should be able to profitably call triples_choices to reduce the size of our branch
                if join_args:
                    # We use the last-added join_arg. It should be the smallest at this point
                    last_join = join_args[-1]
                    if last_join:
                        tl = (list(last_join),)
                        if idx == 2:
                            qx = search_triple[:2] + tl
                        else:
                            qx = tl + search_triple[1:3]
                        trips = self.triples_choices(qx)
                    else: # triples_choices treats [] as wildcard, but for us it's a 'match nothing', so...
                        trips = iter(())
                else:
                    qx = search_triple[:3]
                    trips = self.triples(qx)
            seen = set(y[idx] for y in trips)
            L.debug("Done with {} {}".format(qx, len(seen)))
        finally:
            join_args.append(seen)

    def score(self, hop):
        if self.hop_scorer is not None:
            return self.hop_scorer(hop)
        return 0

    def triples_choices(self, query_triple):
        return self.graph.triples_choices(query_triple)

    def triples(self, query_triple):
        return self.graph.triples(query_triple)

    def __call__(self):
        return self.do_query()


def _format_paths(paths):
    sio = six.StringIO()
    for path in paths:
        for triple in path:
            idx = triple.index(None)
            other_idx = 0 if (idx == 2) else 2
            direction = '' if idx == 2 else '^'
            print('->' + str(triple[1]) + direction + '->' + str(triple[other_idx]), file=sio, end='')
        print(file=sio)
    return sio.getvalue()


class TQLayer(object):
    _NADA = object()

    def __init__(self, nxt=None):
        self.next = nxt

    def triples(self, qt, context=None):
        return self.next.triples(qt)

    def triples_choices(self, qt, context=None):
        return self.next.triples_choices(qt)

    def __contains__(self, x):
        return x in self.next

    def __getattr__(self, attr):
        res = getattr(super(TQLayer, self), attr, TQLayer._NADA)
        if res is TQLayer._NADA:
            return getattr(self.next, attr)

    def __repr__(self):
        return FCN(type(self)) + '(' + repr(self.next) + ')'

    def __str__(self):
        return FCN(type(self)) + '(' + str(self.next) + ')'


class TerminalTQLayer(object):

    @property
    def next(self):
        raise AttributeError(str(type(self)) + ' has no next layer')

    @next.setter
    def next(self, val):
        raise AttributeError(str(type(self)) + ' has no next layer')

    def triples(self, qt, context=None):
        raise NotImplementedError()

    def triples_choices(self, qt, context=None):
        raise NotImplementedError()


class RangeTQLayer(TQLayer):

    def triples(self, query_triple, context=None):
        if isinstance(query_triple[2], _Range):
            in_range = query_triple[2]
            if in_range.defined:
                if getattr(self.next, 'supports_range_queries', False):
                    return self.next.triples(query_triple, context)
                else:
                    qt = (query_triple[0], query_triple[1], None)
                    return set(x for x in self.next.triples(qt, context) if in_range(x[2]))
            else:
                qt = (query_triple[0], query_triple[1], None)
                return self.next.triples(qt, context)
        else:
            return self.next.triples(query_triple, context)

    def triples_choices(self, query_triple, context=None):
        if isinstance(query_triple[2], _Range):
            in_range = query_triple[2]
            qt = (query_triple[0], query_triple[1], None)
            if in_range.defined:
                # XXX: Assuming triples_choices does not also support range
                # queries.
                return set(x for x in self.next.triples_choices(qt, context) if in_range(x[2]))
            else:
                return self.next.triples_choices(qt, context)
        else:
            return self.next.triples_choices(query_triple, context)


class ZeroOrMoreTQLayer(TQLayer):
    def __init__(self, transformer, *args):
        '''
        Parameters
        ----------
        transformer : `callable`
            Returns an object describing the relationship or `None`.
            If an object is returned it must have `predicate`, `identifier`, and
            `direction` attributes.
            - `identifier` is the identifier to start from
            - `predicate` is the predicate to traverse
            - `direction` is the direction of traversal: Either
              `~owmeta_core.rdf_utils.DOWN` for subject -> object or `~owmeta_core.rdf_utils.UP`
              for object -> subject
        *args : other arguments
            Go to `TQLayer` init
        '''
        super(ZeroOrMoreTQLayer, self).__init__(*args)
        self._tf = transformer

    def triples(self, query_triple, context=None):
        i, match = self._find_match(query_triple)
        if not match:
            return self.next.triples(query_triple, context)
        qx = list(query_triple)
        qx[i] = list(transitive_subjects(self.next,
                                         match.identifier,
                                         match.predicate,
                                         context,
                                         match.direction))
        return self._zom_result_helper(qx, match, i, context)

    def _zom_result_helper(self, qx, match, i, context):
        qx = tuple(qx)
        zomses = dict()
        direction = DOWN if match.direction is UP else DOWN
        predicate = match.predicate
        L.debug('ZeroOrMoreTQLayer: start %s zoms %s', match, qx[i])
        for tr in self.next.triples_choices(qx, context):
            zoms = zomses.get(tr[i])
            if zoms is None:
                zoms = [sub for sub in transitive_subjects(self.next, tr[i], predicate, context, direction)]
                zomses[tr[i]] = zoms
            for z in zoms:
                yield tuple(x if x is not tr[i] else z for x in tr)

    def triples_choices(self, query_triple, context=None):
        i, match = self._find_match(query_triple)
        if not match:
            return self.next.triples_choices(query_triple, context)
        qx = list(query_triple)
        iters = []
        # XXX: We should, maybe, apply some stats or heuristics here to determine which list to iterate over.
        for sub in transitive_subjects(self.next,
                                       match.identifier,
                                       match.predicate,
                                       context,
                                       match.direction):
            qx[i] = sub
            iters.append(self.next.triples_choices(tuple(qx), context))
        ch = chain(*iters)
        return ch

    def __contains__(self, query_triple):
        try:
            next(self.triples(query_triple))
            return True
        except StopIteration:
            return False

    def _find_match(self, query_triple):
        match = None
        for i, x in enumerate(query_triple):
            match = self._tf(x)
            if match:
                break
        else: # no break
            return None, None
        return i, match


_default_tq_layers_list = [
    RangeTQLayer,
]


def _default_tq_layers(base):
    res = base
    for layer in reversed(_default_tq_layers_list):
        res = layer(res)
    return res


class ComponentTripler(object):

    """ Gets a set of triples that are connected to the given object by
    objects which have an identifier.

    The ComponentTripler does not query against a backing graph, but instead
    uses the properties attached to the object.
    """

    def __init__(self, start, traverse_undefined=False, generator=False):
        self.start = start
        self.seen = set()
        self.generator = generator
        self.traverse_undefined = traverse_undefined

    def g(self, current_node, i=0):
        if not self.see_node(current_node):
            if self.traverse_undefined or current_node.defined:
                for x in chain(self.recurse_upwards(current_node, i),
                               self.recurse_downwards(current_node, i)):
                    yield x

    def recurse_upwards(self, current_node, depth):
        for prop in current_node.owner_properties:
            for x in self.recurse(prop.owner, prop, current_node, UP, depth):
                yield x

    def recurse_downwards(self, current_node, depth):
        for prop in current_node.properties:
            for val in prop.values:
                for x in self.recurse(current_node, prop, val, DOWN, depth):
                    yield x

    def recurse(self, lhs, via, rhs, direction, depth):
        (ths, nxt) = (rhs, lhs) if direction is UP else (lhs, rhs)
        if self.traverse_undefined or nxt.defined:
            yield (lhs.idl, via.link, rhs.idl)
            for x in self.g(nxt, depth + 1):
                yield x

    def see_node(self, node):
        node_id = id(node)
        if node_id in self.seen:
            return True
        else:
            self.seen.add(node_id)
            return False

    def __call__(self):
        x = self.g(self.start)
        if self.generator:
            return x
        else:
            return set(x)


class _QueryPathElement(tuple):

    def __new__(cls):
        return tuple.__new__(cls, ([], []))

    @property
    def subpaths(self):
        return self[0]

    @subpaths.setter
    def subpaths(self, toset):
        del self[0][:]
        self[0].extend(toset)

    @property
    def path(self):
        return self[1]


class _QueryPreparer(object):

    def __init__(self, start):
        self.seen = list()
        self.stack = list()
        self.paths = list()
        self.start = start
        self.variables = dict()
        self.vcount = 0
        # TODO: Refactor. The return values are not actually
        # used for anything

    def gather_paths_along_properties(
            self,
            current_node,
            property_list,
            direction):
        L.debug("gpap: current_node %s", current_node)
        ret = []
        is_good = False
        for this_property in property_list:
            L.debug("this_property is %s", this_property)
            if direction is UP:
                others = [this_property.owner]
            else:
                others = this_property.values

            for other in others:
                other_id = other.idl

                if isinstance(other, InRange):
                    other_id = _Range(other.min_value, other.max_value)
                elif not other.defined:
                    other_id = self.var(other_id)

                if direction is UP:
                    self.stack.append((other_id, this_property.link, None,
                                      current_node))
                else:
                    self.stack.append((None, this_property.link, other_id,
                                      current_node))
                L.debug("gpap: preparing %s from %s", other, this_property)
                subpath = self.prepare(other)

                if len(self.stack) > 0:
                    self.stack.pop()

                if subpath[0]:
                    is_good = True
                    subpath[1].path.insert(
                        0, (current_node.idl, this_property, other.idl))
                    ret.insert(0, subpath[1])

        L.debug("gpap: exiting %s", "good" if is_good else "bad")
        return is_good, ret

    def var(self, v):
        if v in self.variables:
            return self.variables[v]
        else:
            var = Variable(self.vcount)
            self.variables[v] = var
            self.vcount += 1
            return var

    def prepare(self, current_node):
        L.debug("prepare: current_node %s", repr(current_node))
        if current_node.defined or isinstance(current_node, InRange):
            if len(self.stack) > 0:
                self.paths.append(list(self.stack))
            return True, _QueryPathElement()
        else:
            if current_node in self.seen:
                return False, _QueryPathElement()
            else:
                self.seen.append(current_node)
            owner_parts = self.gather_paths_along_properties(
                current_node,
                current_node.owner_properties,
                UP)
            owned_parts = self.gather_paths_along_properties(
                current_node,
                current_node.properties,
                DOWN)

            self.seen.pop()
            subpaths = owner_parts[1] + owner_parts[1]
            if len(subpaths) == 1:
                ret = subpaths[0]
            else:
                ret = _QueryPathElement()
                ret.subpaths = subpaths
            return (owner_parts[0] or owned_parts[0], ret)

    def __call__(self):
        x = self.prepare(self.start)
        L.debug("self.prepare() result:" + str(x))
        L.debug("_QueryPreparer paths:" + str(_format_paths(self.paths)))
        return self.paths


class DescendantTripler(object):

    """ Gets triples that the object points to, optionally transitively. """

    def __init__(self, start, graph=None, transitive=True):
        """
        Parameters
        ----------
        start : GraphObject
            the node to start from
        graph : rdflib.graph.Graph, optional
            if given, the graph to draw descedants from. Otherwise the object
            graph is used
        """
        self.seen = set()
        self.seen_edges = set()
        self.start = start
        self.graph = graph
        self.results = list()
        self.transitve = transitive

    def g(self, current_node):
        if current_node in self.seen:
            return
        else:
            self.seen.add(current_node)

        if not current_node.defined:
            return

        if self.graph is not None:
            for triple in self.graph.triples((current_node.idl, None, None)):
                self.results.append(triple)
                if self.transitve:
                    self.g(_DTWrapper(triple[2]))
        else:
            for e in current_node.properties:
                if id(e) not in self.seen_edges:
                    self.seen_edges.add(id(e))
                    for val in e.values:
                        if val.defined:
                            self.results.append((current_node.idl, e.link, val.idl))
                            if self.transitve:
                                self.g(val)

    def __call__(self):
        self.g(self.start)
        return self.results


class _DTWrapper():
    """ Used by DescendantTripler to wrap identifiers in GraphObjects """
    defined = True
    __slots__ = ['idl']

    def __init__(self, ident):
        self.idl = ident

    def __hash__(self):
        return hash(self.idl)

    def __eq__(self, other):
        if type(other) == type(self):
            return (other is self) or (other.idl == self.idl)
        else:
            return False


class LegendFinder(object):

    """ Gets a list of the objects which can not be deleted freely from the
    transitive closure.

    Essentially, this is the 'mark' phase of the "mark-and-sweep" garbage
    collection algorithm.

    "Heroes get remembered, but legends never die."
    """

    def __init__(self, start, graph=None):
        self.talked_about = dict()
        self.seen = set()
        self.start = start
        self.graph = graph

    def legends(self, o, depth=0):
        if o in self.seen:
            return
        self.seen.add(o)
        for prop in o.properties:
            for value in prop.values:
                if value != self.start:
                    count = self.count(value)
                    self.talked_about[value] = count - 1
                    self.legends(value, depth + 1)

    def count(self, o):
        if o in self.talked_about:
            return self.talked_about[o]
        else:
            i = 0
            if self.graph is not None:
                for _ in self.graph.triples((None, None, o.idl)):
                    i += 1
            else:
                for prop in o.owner_properties:
                    if prop.owner.defined:
                        i += 1
            return i

    def __call__(self):
        self.legends(self.start)
        return {x for x in self.talked_about if self.talked_about[x] > 0}


class HeroTripler(object):

    def __init__(self, start, graph=None, legends=None):
        self.seen = set()
        self.start = start
        self.heroslist = set()
        self.results = set()
        self.graph = graph

        if legends is None:
            self.legends = LegendFinder(self.start, graph)()
        else:
            self.legends = legends

    def isLegend(self, o):
        return o in self.legends

    def isHero(self, o):
        return o in self.heroslist

    def heros(self, o, depth=0):
        if o in self.seen:
            return
        self.seen.add(o)

        for prop in o.properties:
            for value in prop.values:
                if not self.isLegend(value):
                    self.heros(value, depth + 1)
                    self.hero(value)

    def hero(self, o):
        if not self.isHero(o):
            if self.graph is not None:
                for trip in self.graph.triples((o.idl, None, None)):
                    self.results.add(trip)
            else:
                for e in o.properties:
                    for val in e.values:
                        if val.defined:
                            self.results.add((o.idl, e.link, val.idl))
            self.heroslist.add(o)

    def __call__(self):
        self.heros(self.start)
        self.hero(self.start)
        return self.results


class ReferenceTripler(object):

    def __init__(self, start, graph=None):
        self.seen = set()
        self.seen_edges = set()
        self.start = start
        self.results = set()
        self.graph = graph

    def refs(self, o):
        if self.graph is not None:
            for trip in chain(
                self.graph.triples(
                    (None, None, o.idl)),
                self.graph.triples(
                    (o.idl, None, None))):
                self.results.add(trip)
        else:
            for e in o.properties:
                if (DOWN, id(e)) not in self.seen_edges:
                    self.seen_edges.add((DOWN, id(e)))
                    for val in e.values:
                        if val.defined:
                            self.results.add((o.idl, e.link, val.idl))

            for e in o.owner_properties:
                if (UP, id(e)) not in self.seen_edges:
                    self.seen_edges.add((UP, id(e)))
                    if e.owner.defined:
                        self.results.add((e.owner.idl, e.link, o.idl))

    def __call__(self):
        self.refs(self.start)
        return self.results


class IdentifierMissingException(Exception):

    """ Indicates that an identifier should be available for the object in
        question, but there is none """

    def __init__(self, dataObject="[unspecified object]", *args, **kwargs):
        super(IdentifierMissingException, self).__init__(
            "An identifier should be provided for {}".format(str(dataObject)),
            *args,
            **kwargs)
