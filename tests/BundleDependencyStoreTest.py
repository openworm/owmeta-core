from rdflib.term import URIRef
from rdflib.plugins.memory import IOMemory

from owmeta_core.bundle import BundleDependencyStore


def test_excludes_no_triples():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 0 == sum(1 for _ in bds.triples((None, None, None)))


def test_excludes_some_triples():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 1 == sum(1 for _ in bds.triples((None, None, None)))


def test_excludes_all_for_excluded_context():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 0 == sum(1 for _ in bds.triples((None, None, None),
                                           context='http://example.org/ctx'))


def test_includes_triples():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom)
    assert 1 == sum(1 for _ in bds.triples((None, None, None)))
