import logging
import rdflib

L = logging.getLogger(__name__)

__all__ = ["DatatypePropertyMixin",
           "UnionPropertyMixin"]


class UnionPropertyMixin(object):

    """ A Property that can handle either DataObjects or basic types """

    def __init__(self, resolver, **kwargs):
        """
        Parameters
        ----------
        resolver : RDFTypeResolver
            Resolves RDF identifiers into objects returned from :meth:`get`
        """
        super(UnionPropertyMixin, self).__init__(**kwargs)
        self.resolver = resolver

    def set(self, v):
        return super(UnionPropertyMixin, self).set(v)

    def get(self):
        for ident in super(UnionPropertyMixin, self).get():
            if isinstance(ident, rdflib.Literal):
                yield self.resolver.deserializer(ident)
            elif isinstance(ident, rdflib.BNode):
                L.warn(
                    'UnionProperty.get: Retrieved BNode, "' +
                    ident +
                    '". BNodes are not supported in owmeta-core')
            else:
                types = set()
                rdf = super(UnionPropertyMixin, self).rdf
                for rdf_type in rdf.objects(ident, rdflib.RDF['type']):
                    types.add(rdf_type)
                L.debug("{} <- types, {} <- ident".format(types, ident))
                the_type = self.resolver.base_type
                if len(types) == 0:
                    L.warn(
                        'UnionProperty.get: Retrieved un-typed URI, "' +
                        ident +
                        '", for a DataObject. Creating a default-typed object')
                else:
                    the_type = self.resolver.type_resolver(
                            self.owner.context.rdf_graph(), types)
                    L.debug("the_type = {}".format(the_type))

                yield self.resolver.id2ob(ident, the_type, context=self.owner.context)
