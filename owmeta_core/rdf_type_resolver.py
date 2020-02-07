class RDFTypeResolver(object):

    def __init__(self, default_type, type_resolver, id2object_translator, deserializer):
        """
        Parameters
        ----------
        default_type : :class:`str`, :class:`rdflib.term.URIRef`
            If no type is retrieved from the graph, this will be the type selected
        type_resolver : callable([:class:`rdflib.term.URIRef`]) -> :class:`rdflib.term.URIRef`
            This callable (e.g., function) receives all the types found for an
            identifier and returns a single identifier for a type that
            `id2object_translator` can translate into an object
        id2object_translator : callable(:class:`rdflib.term.URIRef`, :class:`rdflib.term.URIRef`) -> :type:`object`
            This callable (e.g., function) receives an identifier for an object
            and an identifier for the object's type and returns an object
            corresponding to the identifier and type
        deserializer : callable(:class:`rdflib.term.Literal`) -> :type:`object`
            This callable (e.g., function) receives a literal and turns it
            into an object
        """
        self.base_type = default_type
        self.type_resolver = type_resolver
        self.id2ob = id2object_translator
        self.deserializer = deserializer
