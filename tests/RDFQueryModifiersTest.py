from rdflib.graph import Graph
from rdflib.namespace import Namespace, RDFS
from owmeta_core.rdf_query_modifiers import (ZeroOrMoreTQLayer,
                                             rdfs_subclassof_subclassof_zom_creator as mod)


ex = Namespace('http://example.org/')


def test_zom_triples_choices():
    g = Graph()
    g.add((ex.a, RDFS.subClassOf, ex.b))
    g.add((ex.b, RDFS.subClassOf, ex.c))
    g.add((ex.c, RDFS.subClassOf, ex.d))
    g.add((ex.d, RDFS.subClassOf, ex.e))
    g.add((ex.e, RDFS.subClassOf, ex.f))
    g.add((ex.f, RDFS.subClassOf, ex.g))
    g = ZeroOrMoreTQLayer(mod(ex.c), g)

    choices = set(g.triples_choices((None, RDFS.subClassOf, [ex.f, ex.c])))
    expected = [(ex.a, RDFS.subClassOf, ex.c),
                (ex.a, RDFS.subClassOf, ex.b),
                (ex.a, RDFS.subClassOf, ex.d),
                (ex.a, RDFS.subClassOf, ex.e),
                (ex.a, RDFS.subClassOf, ex.f),

                (ex.b, RDFS.subClassOf, ex.c),
                (ex.b, RDFS.subClassOf, ex.d),
                (ex.b, RDFS.subClassOf, ex.e),
                (ex.b, RDFS.subClassOf, ex.f),

                (ex.c, RDFS.subClassOf, ex.d),
                (ex.c, RDFS.subClassOf, ex.e),
                (ex.c, RDFS.subClassOf, ex.f),

                (ex.d, RDFS.subClassOf, ex.e),
                (ex.d, RDFS.subClassOf, ex.f),

                (ex.e, RDFS.subClassOf, ex.f)]
    assert choices == set(expected)
