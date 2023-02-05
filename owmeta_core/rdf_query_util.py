import logging

import rdflib

from .graph_object import GraphObjectQuerier
from .rdf_query_modifiers import (ZeroOrMoreTQLayer,
                                  rdfs_subclassof_zom_creator)

L = logging.getLogger(__name__)


def goq_hop_scorer(hop):
    if hop[1] == rdflib.RDF.type:
        return 1
    return 0


def load_base(graph, idents, target_type, context, resolver):
    '''
    Loads a set of objects from an RDF graph given their identifiers

    Parameters
    ----------
    graph : rdflib.graph.Graph
        The graph to query from
    idents : list of rdflib.term.URIRef
        A list of identifiers to convert into objects
    target_type : rdflib.term.URIRef
        URI of the target type. Any result will be a sub-class of this type
    context : object
        Limits the scope of the query to statements within or entailed by this context.
        Notionally, it's a owmeta_core.context.Context instance
    resolver : .rdf_type_resolver.RDFTypeResolver
        Handles some of the mappings
    '''

    L.debug("load_base: graph %s target_type %s context %s resolver %s",
            graph, target_type, context, resolver)
    if not idents:
        return

    grouped_types = dict()
    # We don't use a subclassof ZOM layer for this query since we are going to get the
    # "most specific" type, which will have to be one declared explicitly
    L.debug("querying %s types in %s", idents, graph)
    idents = list(idents)
    ids_missing_types = set(idents)
    for ident, _, rdf_type in graph.triples_choices((idents,
                                                     rdflib.RDF['type'],
                                                     None)):
        t = grouped_types.get(ident, None)
        if t is None:
            ids_missing_types.remove(ident)
            grouped_types[ident] = set([rdf_type])
        else:
            t.add(rdf_type)
    if ids_missing_types:
        raise MissingRDFTypeException('Could not recover a type declaration for'
                                      f' {ids_missing_types}')

    for ident, types in grouped_types.items():
        the_type = resolver.type_resolver(graph, types, base=target_type)
        if the_type is None:
            raise MissingRDFTypeException(
                    f'The type resolver could not recover a type for {ident}'
                    f' from {types} constrained to {target_type}')
        yield resolver.id2ob(ident, the_type, context)


def load_terms(graph, start, target_type):
    '''
    Loads a set of terms based on the object graph starting from `start`

    Parameters
    ----------
    graph : rdflib.graph.Graph
        The graph to query from
    start : .graph_object.GraphObject
        The graph object to start the query from
    target_type : rdflib.term.URIRef
        URI of the target type. Any result will be a sub-class of this type
    '''

    L.debug("load: start %s, target_type %s, graph %s", start, target_type, graph.store)
    graph = ZeroOrMoreTQLayer(rdfs_subclassof_zom_creator(target_type), graph)
    return GraphObjectQuerier(start, graph, hop_scorer=goq_hop_scorer)()


def load(graph, start, target_type, *args):
    '''
    Loads a set of objects based on the graph starting from `start`

    Parameters
    ----------
    graph : rdflib.graph.Graph
        The graph to query from
    start : .graph_object.GraphObject
        The graph object to start the query from
    target_type : rdflib.term.URIRef
        URI of the target type. Any result will be a sub-class of this type
    '''

    idents = load_terms(graph, start, target_type)
    return load_base(graph, idents, target_type, *args)


def get_most_specific_rdf_type(graph, types, base=None):
    '''
    Find the RDF type that isn't a sub-class of any other, constrained to be a sub-class
    of `base` if that is provided.

    Parameters
    ----------
    graph : rdflib.graph.Graph
        The graph to query rdfs:subClassOf relationships
    types : list of rdflib.term.URIRef
        The types to query
    base : rdflib.term.URIRef
        The "base" type

    See Also
    --------
    RDFTypeResolver
    '''
    if len(types) == 1 and (not base or (base,) == tuple(types)):
        return tuple(types)[0]

    if not types and base:
        return base

    most_specific_types = _gmsrt_helper(graph, types, base)

    if len(most_specific_types) == 1:
        return most_specific_types.pop()
    else:
        L.warning(('No most-specific type could be determined among %s'
                   ' constrained to subclasses of %r'), types, base)
        return None


def _gmsrt_helper(graph, start, base=None):
    # Finds the most specific (furthest to the left in the chain of subClassOf
    # relationships), then confirms that the resulting set is a sub-class of the base if
    # one is given
    res = set(start)
    border = set(start)
    subclasses = {s: {s} for s in start}
    hit = True
    while len(res) > 1:
        new_border = set()
        itr = graph.triples_choices((list(border), rdflib.RDFS.subClassOf, None))
        hit = False
        for item in itr:
            if isinstance(item[0], tuple):
                # If we retrieved from a rdflib.store.Store instead of a Graph, then we
                # have to get the first element of the pair `item` to get the actual triple
                triple = item[0]
            else:
                triple = item
            subj, _, obj = triple
            if obj != subj:
                obj_subclasses = subclasses.get(obj, None)
                if obj_subclasses is None:
                    subclasses[obj] = obj_subclasses = set()
                obj_subclasses |= subclasses[subj]
                res.discard(obj)
                hit = True
                new_border.add(obj)
        if not hit:
            break
        border = new_border
    if base is not None:
        if not hit:
            # If hit is False, then we've stopped because no more super-classes were found, so
            # base *has* to be in here or else none of our types are any good
            res &= subclasses.get(base, set())
        else:
            # We exited because we eliminated all of the other types (or only had one to
            # begin with), but keep going to make sure we have the base.
            seen = set(border)
            while border:
                if base in border:
                    break
                border = {o for _, _, o
                        in graph.triples_choices((list(border), rdflib.RDFS.subClassOf, None))
                        if o not in seen}
                seen |= border
            else: # no break
                # We never found the base class in super-classes of our result set, so we
                # have to drop the result set
                res = set()
    return res


def oid(identifier_or_rdf_type, rdf_type, context, base_type=None):
    """
    Create an object from its rdf type

    Parameters
    ----------
    identifier_or_rdf_type : rdflib.term.URIRef
        If `rdf_type` is provided, then this value is used as the identifier
        for the newly created object. Otherwise, this value will be the
        :attr:`rdf_type` of the object used to determine the Python type and
        the object's identifier will be randomly generated.
    rdf_type : rdflib.term.URIRef
        If provided, this will be the :attr:`rdf_type` of the newly created
        object.
    context : Context, optional
        The context to resolve a class from
    base_type : type
        The base type

    Returns
    -------
       The newly created object

    """
    identifier = identifier_or_rdf_type
    if rdf_type is None:
        rdf_type = identifier_or_rdf_type
        identifier = None

    cls = None
    if context is not None:
        cls = context.resolve_class(rdf_type)

    if cls is None and context is not None:
        for types in _superclass_iter(context.rdf_graph(), rdf_type):
            for typ in types:
                cls = context.resolve_class(typ)
                if cls is not None:
                    break
            if cls is not None:
                break

    if cls is None:
        cls = base_type

    # if its our class name, then make our own object
    # if there's a part after that, that's the property name
    if context is not None:
        cls = context(cls)

    if identifier is not None:
        o = cls.query(ident=identifier, no_type_decl=True)
    else:
        o = cls.query()
    return o


def _superclass_iter(graph, start):
    '''
    Generate up the super-classes of this type
    '''
    border = set([start])
    seen = set([start])
    while True:
        new_border = set()

        for t in graph.triples_choices((list(border), rdflib.RDFS.subClassOf, None)):
            if t[2] not in seen:
                new_border.add(t[2])
                seen.add(t[2])

        if border == new_border:
            break
        if not new_border:
            break
        yield new_border
        border = new_border


class MissingRDFTypeException(Exception):
    '''
    Raised when we were looking for an RDF type couldn't find one
    '''
