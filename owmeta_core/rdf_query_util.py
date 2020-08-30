from __future__ import print_function
import logging

import rdflib

from .graph_object import (GraphObjectQuerier,
                           ZeroOrMoreTQLayer)
from .rdf_go_modifiers import SubClassModifier

L = logging.getLogger(__name__)


def goq_hop_scorer(hop):
    if hop[1] == rdflib.RDF.type:
        return 1
    return 0


def zomifier(target_type):
    def helper(rdf_type):
        if target_type == rdf_type:
            return SubClassModifier(rdf_type)
    return helper


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
        the_type = get_most_specific_rdf_type(types, context, base=target_type)
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

    L.debug("load: start %s target_type %s", start, target_type)
    g = ZeroOrMoreTQLayer(zomifier(target_type), graph)
    return GraphObjectQuerier(start, g, parallel=False, hop_scorer=goq_hop_scorer)()


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


def get_most_specific_rdf_type(types, context=None, base=None):
    """ Gets the most specific rdf_type.

    Returns the URI corresponding to the lowest in the DataObject class
    hierarchy from among the given URIs.
    """
    if context is None:
        if len(types) == 1 and (not base or (base,) == tuple(types)):
            return tuple(types)[0]
        if not types and base:
            return base
        msg = "Without a Context, `get_most_specific_rdf_type` cannot order RDF types {}{}".format(
                types,
                " constrained to be subclasses of {}".format(base) if base else '')
        L.warning(msg)
        return None
    most_specific_types = ()
    if base:
        base_class = context.resolve_class(base)
        if base_class:
            most_specific_types = (base_class,)
    for typ in types:
        class_object = context.resolve_class(typ)
        if class_object is None:
            L.warning(
                    f"A Python class corresponding to the type URI {repr(typ)} couldn't be found.")
        elif issubclass(class_object, most_specific_types):
            most_specific_types = (class_object,)

    if len(most_specific_types) == 1:
        return most_specific_types[0].rdf_type
    else:
        L.warning(('No most-specific type could be determined among {}'
                   ' constrained to subclasses of {}').format(types, repr(base)))
        return None


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

    if cls is None:
        if base_type is None:
            from .dataobject import DataObject
            cls = DataObject
        else:
            cls = base_type
    L.debug("oid: making a {} with ident {}".format(cls, identifier))

    # if its our class name, then make our own object
    # if there's a part after that, that's the property name
    o = None

    if context is not None:
        cls = context(cls)

    if identifier is not None:
        o = cls.query(ident=identifier, no_type_decl=True)
    else:
        o = cls.query()
    return o
