class RDFTypeResolver:
    '''
    Handles mapping between RDF graphs and Python types
    '''

    def __init__(self, default_type, type_resolver, id2object_translator, deserializer):
        """
        Parameters
        ----------
        default_type : :class:`str`, :class:`rdflib.term.URIRef`
            If no type is retrieved from the graph, this will be the type selected
        type_resolver : callable : (rdflib.graph.Graph, [rdflib.term.URIRef], rdflib.term.URIRef or None) -> rdflib.term.URIRef
            This callable (e.g., function) receives a graph, all the types found for an
            identifier, and the "base" type sought, which constrains the result to be a
            sub-type of the base, and returns a single identifier for a type that
            `id2object_translator` can translate into an object
        id2object_translator : callable : (rdflib.term.URIRef, rdflib.term.URIRef, owmeta_core.context.Context) -> object
            This callable (e.g., function) receives an identifier for an object
            and an identifier for the object's type and returns an object
            corresponding to the identifier and type
        deserializer : callable : (:class:`rdflib.term.Literal`) -> object
            This callable (e.g., function) receives a literal and turns it
            into an object
        """
        self.base_type = default_type
        self.type_resolver = type_resolver
        self.id2ob = id2object_translator
        self.deserializer = deserializer
