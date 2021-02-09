from __future__ import print_function
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
    target_type : rdflib.term.URIRef
        URI of the target type. Any result will be a sub-class of this type
    context : .context.Context
        Limits the scope of the query to statements within or entailed by this context
    idents : list of rdflib.term.URIRef
        A list of identifiers to convert into objects
    '''

    L.debug("load_base: graph %s target_type %s context %s resolver %s",
            graph, target_type, context, resolver)
    if not idents:
        return

    grouped_types = dict()
    # We don't use a subclassof ZOM layer for this query since we are going to get the
    # "most specific" type, which will have to be one declared explicitly
    for ident, _, rdf_type in graph.triples_choices((list(idents),
                                                     rdflib.RDF['type'],
                                                     None)):
        t = grouped_types.get(ident, None)
        if t is None:
            grouped_types[ident] = set([rdf_type])
        else:
            t.add(rdf_type)

    hit = False
    for ident, types in grouped_types.items():
        hit = True
        the_type = get_most_specific_rdf_type(graph, types, base=target_type)
        if the_type is None:
            raise Exception(f'Could not recover a type for {ident}')
        yield resolver.id2ob(ident, the_type, context)

    if not hit:
        for ident in idents:
            the_type = None
            if target_type:
                the_type = target_type
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

    L.debug("load: start %s, target_type %s, graph %s", start, target_type, graph)
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
    Find the rdf type that isn't a sub-class of any other, constrained to `base` if that
    is provided.
    '''
    if len(types) == 1 and (not base or (base,) == tuple(types)):
        return tuple(types)[0]

    if not types and base:
        return base

    most_specific_types = _gmsrt_helper(graph, types, base)

    if len(most_specific_types) == 1:
        return most_specific_types.pop()
    else:
        L.warning(('No most-specific type could be determined among {}'
                   ' constrained to subclasses of {}').format(types, repr(base)))
        return None


def _gmsrt_helper(graph, start, base=None):
    res = set(start)
    border = set(start)
    colors = {s: {s} for s in start}
    hit = True
    while len(res) > 1:
        new_border = set()
        itr = graph.triples_choices((list(border), rdflib.RDFS.subClassOf, None))
        hit = False
        for t in itr:
            if isinstance(t[0], tuple):
                t = t[0]
            o = t[2]
            s = t[0]
            if o != s:
                o_color = colors.get(o, None)
                if o_color is None:
                    colors[o] = o_color = set()
                o_color |= colors[s]
                res.discard(o)
                hit = True
                new_border.add(o)
        if not hit:
            break
        border = new_border
    if base is not None:
        if not hit:
            # If hit is False, then we've stopped because no more super-classes were found, so
            # base *has* to be in here or else none of our types are any good
            res &= colors.get(base, set())
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
    return res


def oid(identifier_or_rdf_type=None, rdf_type=None, context=None, base_type=None):
    """
    Create an object from its rdf type

    Parameters
    ----------
    identifier_or_rdf_type : :class:`str` or :class:`rdflib.term.URIRef`
        If `rdf_type` is provided, then this value is used as the identifier
        for the newly created object. Otherwise, this value will be the
        :attr:`rdf_type` of the object used to determine the Python type and
        the object's identifier will be randomly generated.
    rdf_type : :class:`str`, :class:`rdflib.term.URIRef`, :const:`False`
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
        if base_type is None:
            from .dataobject import BaseDataObject
            cls = BaseDataObject
        else:
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
