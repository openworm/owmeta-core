from itertools import chain
import logging

import rdflib as R
from rdflib.namespace import RDFS, RDF

from .utils import FCN
from .rdf_utils import UP, DOWN, transitive_subjects
from .ranged_objects import InRange


L = logging.getLogger(__name__)


class ZeroOrMore(object):
    def __init__(self, identifier, predicate, index, direction=DOWN):
        self.identifier = identifier
        self.predicate = predicate
        self.direction = direction
        self.direction = direction
        self.index = index

    def __repr__(self):
        return "{}({}, {}, {})".format(FCN(type(self)),
                                       repr(self.identifier),
                                       repr(self.predicate),
                                       repr(self.direction))


class SubClassModifier(ZeroOrMore):

    def __init__(self, rdf_type):
        super().__init__(rdf_type, R.RDFS.subClassOf, 2, UP)

    def __repr__(self):
        return FCN(type(self)) + '(' + repr(self.identifier) + ')'


class SubPropertyOfModifier(ZeroOrMore):

    def __init__(self, rdf_property):
        super().__init__(rdf_property, R.RDFS.subPropertyOf, 1, direction=UP)

    def __repr__(self):
        return FCN(type(self)) + '(' + repr(self.identifier) + ')'


def rdfs_subclassof_zom_creator(target_type):
    '''
    Creates a function used by `ZeroOrMoreTQLayer` to determine if a query needs to be
    augmented to retrieve sub-classes of a *given* RDF type
    '''
    def helper(triple):
        if target_type == triple[2] is not None and triple[1] == R.RDF.type:
            return SubClassModifier(triple[2])
    return helper


def rdfs_subpropertyof_zom(super_property):
    '''
    Argument to `ZeroOrMoreTQLayer`. Adds sub-properties of the given property to triple
    queries
    '''
    def helper(triple):
        if triple[1] == super_property:
            return SubPropertyOfModifier(super_property)
    return helper


def rdfs_subclassof_zom(triple):
    '''
    Argument to `ZeroOrMoreTQLayer`. Adds sub-classes to triple queries for an rdf:type
    '''
    if triple[2] is not None and triple[1] == R.RDF.type:
        return SubClassModifier(triple[2])


def rdfs_subclassof_subclassof_zom_creator(rdf_type):
    def helper(triple):
        '''
        Argument to `ZeroOrMoreTQLayer`. Adds sub-classes to triple queries for an
        rdfs:subClassOf
        '''
        if (triple[1] == R.RDFS.subClassOf and
                triple[2] is not None and
                (rdf_type == triple[2] or
                    (isinstance(triple[2], list) and rdf_type in triple[2]))):
            return SubClassModifier(triple[2])
    return helper


class TQLayer(object):
    '''
    Triple Query Layer. Wraps a graph or another `TQLayer` to do something to the
    `triples` and `triples_choices` query or the result of the query.
    '''
    _NADA = object()

    def __init__(self, nxt=None):
        '''
        Parameters
        ----------
        nxt : TQLayer or rdflib.graph.Graph
            The "next" or "lower" layer that this layer modifies
        '''
        self.next = nxt

    def triples(self, qt, context=None):
        return self.next.triples(qt)

    def triples_choices(self, qt, context=None):
        return self.next.triples_choices(qt)

    def __contains__(self, qt):
        '''
        This should be overridden -- the default implementation just asks the next layer
        if it contains the triple.
        '''
        return qt in self.next

    def __getattr__(self, attr):
        '''
        By default, if this layer doesn't have the given attribute, then the attribute
        will be looked up on the next layer.
        '''
        res = getattr(super(TQLayer, self), attr, TQLayer._NADA)
        if res is TQLayer._NADA:
            return getattr(self.next, attr)

    def __repr__(self):
        return FCN(type(self)) + '(' + repr(self.next) + ')'

    def __str__(self):
        return FCN(type(self)) + '(' + str(self.next) + ')'


class TerminalTQLayer(object):
    '''
    A TQLayer that has no "next". May be useful to create a layer that stands in place of
    a `~rdflib.graph.Graph`.
    '''

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
    '''
    A layer that understands ranges in the object position of a triple.

    If the next layer has the `supports_range_queries` attribute set to `True`, then the
    range is passed down as-is
    '''

    def triples(self, query_triple, context=None):
        if isinstance(query_triple[2], InRange):
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
        if isinstance(query_triple[2], InRange):
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
            Takes a triple and returns an object describing the relationship or `None`.
            If an object is returned it must have `predicate`, `identifier`,
            `direction`, and `index` attributes.
            - `identifier` is the identifier to start from
            - `predicate` is the predicate to traverse
            - `direction` is the direction of traversal: Either
              `~owmeta_core.rdf_utils.DOWN` for subject -> object or `~owmeta_core.rdf_utils.UP`
              for object -> subject
            - `index` is the index in the triple for which a closure should be looked up
        *args : other arguments
            Go to `TQLayer` init
        '''
        super(ZeroOrMoreTQLayer, self).__init__(*args)
        self._tf = transformer

    def triples(self, query_triple, context=None):
        match = self._tf(query_triple)
        if not match:
            return self.next.triples(query_triple, context)
        qx = list(query_triple)
        if match.identifier is not None:
            matches = list(transitive_subjects(self.next,
                                             match.identifier,
                                             match.predicate,
                                             context,
                                             match.direction))
            qx[match.index] = matches
        results = self.next.triples_choices(tuple(qx), context)
        return self._zom_result_helper(results, match, context, set(matches))

    def triples_choices(self, query_triple, context=None):
        match = self._tf(query_triple)
        if not match:
            L.debug('No match %s', query_triple)
            return self.next.triples_choices(query_triple, context)
        qx = list(query_triple)
        iters = []
        # XXX: We should, maybe, apply some stats or heuristics here to determine which list to iterate over.
        if isinstance(match.identifier, list):
            assert isinstance(qx[match.index], list), ('If there is more than one'
            ' matching identifier, the list in the query triple must be in the same'
            ' position')

            matches = set(qx[match.index])
            for match_id in match.identifier:
                matches |= set(transitive_subjects(self.next,
                                                   match_id,
                                                   match.predicate,
                                                   context,
                                                   match.direction,
                                                   matches))
            qx[match.index] = list(matches)
            iters.append(self.next.triples_choices(tuple(qx), context))
        elif match.identifier is not None:
            matches = set(transitive_subjects(self.next,
                                              match.identifier,
                                              match.predicate,
                                              context,
                                              match.direction))
            for sub in matches:
                qx[match.index] = sub
                iters.append(self.next.triples_choices(tuple(qx), context))
        else:
            matches = None
            iters.append(self.next.triples_choices(query_triple, context))
        return self._zom_result_helper(chain(*iters), match, context, matches)

    def _zom_result_helper(self, results, match, context, limit):
        zomses = dict()
        direction = DOWN if match.direction is UP else DOWN
        predicate = match.predicate
        index = match.index
        L.debug('ZeroOrMoreTQLayer: start %s', match)
        # The results from the original query are augmented here to "entail" results in
        # the "reverse" direction that are implied by the "forward" direction. For
        # instance, if I request everything with a type that's rdfs:Resource, I'll get all
        # type statements we have subclass relationships for with the modified query, but
        # I'll be missing the inferred types. We rectify that below
        #
        for tr in results:
            zoms = zomses.get(tr[index])
            if zoms is None:
                if limit:
                    zoms = set(transitive_subjects(self.next, tr[index], predicate, context, direction)) & limit
                else:
                    zoms = set(transitive_subjects(self.next, tr[index], predicate, context, direction))
                zomses[tr[index]] = zoms
            for z in zoms:
                yield tuple(x if x is not tr[index] else z for x in tr)

    def __contains__(self, query_triple):
        try:
            next(self.triples(query_triple))
            return True
        except StopIteration:
            return False


class ContainerMembershipIsMemberTQLayer(TQLayer):
    '''
    Adds a triple into the results for rdfs:subPropertyOf(rdfs:member) relationships for all
    known ContainerMembershipProperty instances
    '''
    def triples(self, query_triple, context=None):
        iters = [self.next.triples(query_triple, context)]
        if query_triple[1] == RDFS.subPropertyOf and query_triple[2] == RDFS.member:
            iters.append((t[0], RDFS.subPropertyOf, RDFS.member)
                    for t in self.next.triples((None, RDF.type, RDFS.ContainerMembershipProperty), context))
        return chain(*iters)

    def triples_choices(self, query_triple, context=None):
        iters = [self.next.triples_choices(query_triple, context)]
        if (query_triple[1] == RDFS.subPropertyOf and
                (query_triple[2] == RDFS.member or
                    (isinstance(query_triple[2], list) and RDFS.member in query_triple[2]))):
            iters.append((t[0], RDFS.subPropertyOf, RDFS.member)
                    for t in self.next.triples((None, RDF.type, RDFS.ContainerMembershipProperty), context))
        return chain(*iters)


_default_tq_layers_list = [
    RangeTQLayer,
]


def default_tq_layers(base):
    res = base
    for layer in reversed(_default_tq_layers_list):
        res = layer(res)
    return res
