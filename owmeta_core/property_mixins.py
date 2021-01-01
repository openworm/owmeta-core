import logging
import rdflib

L = logging.getLogger(__name__)

__all__ = ["DatatypePropertyMixin",
           "ObjectPropertyMixin",
           "UnionPropertyMixin"]


class DatatypePropertyMixin(object):

    def __init__(self, resolver, **kwargs):
        """
        Parameters
        ----------
        resolver : RDFTypeResolver
            Resolves RDF identifiers returned from :meth:`get` into objects
        """
        super(DatatypePropertyMixin, self).__init__(**kwargs)
        self.resolver = resolver

    def set(self, v):
        return super(DatatypePropertyMixin, self).set(v)

    def get(self):
        for val in super(DatatypePropertyMixin, self).get():
            yield self.resolver.deserializer(val)


class ObjectPropertyMixin(object):

    def __init__(self, resolver, **kwargs):
        """
        Parameters
        ----------
        resolver : RDFTypeResolver
            Resolves RDF identifiers returned from :meth:`get` into objects
        """
        super(ObjectPropertyMixin, self).__init__(**kwargs)
        self.resolver = resolver

    def set(self, v):
        if not hasattr(v, 'idl'):
            raise Exception("An ObjectProperty value must have an attribute named 'idl': Got {}".format(v))
        return super(ObjectPropertyMixin, self).set(v)

    def get(self):
        for ident in super(ObjectPropertyMixin, self).get():
            n = self.id2ob(ident)
            if n:
                yield n

    def id2ob(self, ident):
        if not isinstance(ident, rdflib.URIRef):
            L.warn(
                'ObjectProperty.get: Skipping non-URI term, "' +
                str(ident) +
                '", returned for a DataObject.')
            return None

        types = set()
        types.add(type(self).value_rdf_type)
        sup = super(ObjectPropertyMixin, self)
        if hasattr(sup, 'rdf'):
            for rdf_type in sup.rdf.objects(
                    ident, rdflib.RDF['type']):
                types.add(rdf_type)
        else:
            L.warn('ObjectProperty.get: base type is missing an "rdf"'
                   ' property. Retrieved values will be created as ' +
                   str(type(self).value_rdf_type))

        the_type = self.resolver.type_resolver(types)
        return self.resolver.id2ob(ident, the_type)


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
                    the_type = self.resolver.type_resolver(types)
                    L.debug("the_type = {}".format(the_type))

                yield self.resolver.id2ob(ident, the_type, context=self.owner.context)
