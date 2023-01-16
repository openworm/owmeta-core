'''
This example demonstrates a way to use alternative objects to DataObject while still using
Contexts and other facilities for RDF<->object mapping
'''
import rdflib as R
from rdflib.graph import Graph
from rdflib.term import URIRef
from owmeta_core.rdf_query_util import load_base, get_most_specific_rdf_type
from owmeta_core.rdf_utils import deserialize_rdflib_term
from owmeta_core.context import Context
from owmeta_core.rdf_type_resolver import RDFTypeResolver
from owmeta_core.statement import Statement


# You could implement .idl as a property on your own objects, otherwise you'll need an
# adapter like this to make statements
class DOAdapter:
    def __init__(self, identifier):
        self.idl = URIRef(identifier)


# This is just how I'm choosing to implement properties, you don't have to use Contexts to
# accumulate statements or any of that.
class D2Property:
    rdf_property = None

    def __init__(self, context=None):
        self.__context = context

    @property
    def context(self):
        return self.__context

    @property
    def link(self):
        return URIRef(self.rdf_property)

    def contextualize(self, ctx):
        if self.__context is not None:
            raise Exception('Cannot recontextualize')
        self.__context = ctx
        return self

    def __call__(self, a, b):
        self.context.add_statement(
                Statement(DOAdapter(a.identifier),
                          self,
                          DOAdapter(b.identifier),
                          self.context))


class P1(D2Property):
    rdf_property = 'http://example.org/property/P1'


class RDFType(D2Property):
    rdf_property = R.RDF.type


class RDFSSubClassOf(D2Property):
    rdf_property = R.RDFS.subClassOf


class Resource:
    rdf_type = R.RDFS['Resource']

    def __init__(self, identifier, context=None):
        self.identifier = identifier
        self.context = context
        if self.rdf_type == identifier:
            RDFType(context=context)(self, self)
        else:
            RDFType(context=context)(self, RDFSClass(self.rdf_type, context))

    @classmethod
    def rdf_object(self, context=None):
        return RDFSClass(self.rdf_type, context=context)

    def __str__(self):
        return f'{type(self).__name__}({self.identifier})'


class RDFSClass(Resource):
    rdf_type = R.RDFS['Class']


class B(Resource):
    rdf_type = URIRef('http://example.org/class/B')


_RDF_TYPES = {URIRef(x.rdf_type): x for x in (B, RDFSClass, Resource)}


def id2ob(ident, rdf_type, context):
    cls = _RDF_TYPES.get(rdf_type)
    if cls is not None:
        return cls(ident, context=context)

    raise TypeError(f'No class found for {rdf_type} in {context}')


rdf_type_resolver = RDFTypeResolver(Resource.rdf_type,
                get_most_specific_rdf_type,
                id2ob,
                deserialize_rdflib_term)

ctx1 = Context('http://example.org/ctx1')

# We could automate these sub-class declarations, but it would typically require some
# metaclass stuff that would clutter this short example.
subClassOf = RDFSSubClassOf(context=ctx1)
subClassOf(B.rdf_object(context=ctx1), Resource.rdf_object(context=ctx1))

# Declaring info using our alternative to DataObject
p1 = P1(context=ctx1)
a1 = Resource('http://example.org/ob/a1', context=ctx1)
b1 = B('http://example.org/ob/b1', context=ctx1)
p1(a1, b1)

ctx1.save(g := Graph())
print(g.serialize())

# Here we load objects back from the graph.
#
# We left out a query to produce the two URIRefs below, but we could have done queries any
# way we want
for m in load_base(g, [URIRef('http://example.org/ob/a1'), URIRef('http://example.org/ob/b1')],
                   Resource.rdf_type, ctx1, rdf_type_resolver):
    print(m)
