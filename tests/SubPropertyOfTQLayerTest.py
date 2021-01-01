from rdflib.namespace import RDFS
from rdflib.term import URIRef
from rdflib.graph import Graph

from owmeta_core.rdf_query_modifiers import (ZeroOrMoreTQLayer,
                                             rdfs_subpropertyof_zom)


def test_triples():
    graph = Graph()
    graph.add((u('a'), u('b'), u('c')))
    graph.add((u('b'), RDFS.subPropertyOf, u('d')))

    cut = ZeroOrMoreTQLayer(rdfs_subpropertyof_zom(u('d')), graph)
    assert set([(u('a'), u('d'), u('c')), (u('a'), u('b'), u('c'))]) == set(cut.triples((u('a'), u('d'), u('c'))))


def test_triples_upper_bound_results():
    '''
    When expanding sub-property relationships, we don't want to include in the results
    super-properties that exceed the requested property
    '''
    graph = Graph()
    graph.add((u('a'), u('b'), u('c')))
    graph.add((u('b'), RDFS.subPropertyOf, u('d')))
    graph.add((u('b'), RDFS.subPropertyOf, u('e')))

    cut = ZeroOrMoreTQLayer(rdfs_subpropertyof_zom(u('d')), graph)
    assert set([(u('a'), u('d'), u('c')), (u('a'), u('b'), u('c'))]) == set(cut.triples((u('a'), u('d'), u('c'))))


def test_triples_choices():
    graph = Graph()
    graph.add((u('a'), u('b'), u('c')))
    graph.add((u('a'), u('e'), u('f')))
    graph.add((u('b'), RDFS.subPropertyOf, u('d')))
    graph.add((u('e'), RDFS.subPropertyOf, u('d')))

    cut = ZeroOrMoreTQLayer(rdfs_subpropertyof_zom(u('d')), graph)
    expected = [(u('a'), u('d'), u('f')),
                (u('a'), u('e'), u('f')),
                (u('a'), u('d'), u('c')),
                (u('a'), u('b'), u('c'))]
    actual = cut.triples_choices((u('a'), u('d'), [u('c'), u('f')]))
    assert set(expected) == set(actual)


def u(t):
    return URIRef(f'http://example.org/{t}')
