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


def test_includes_contexts():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom)
    assert set(['http://example.org/ctx']) == set(bds.contexts())


def test_excludes_contexts():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set([]) == set(bds.contexts())


def test_excludes_some_contexts1():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx2')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set(['http://example.org/ctx2']) == set(bds.contexts())


def test_excludes_some_contexts2():
    iom = IOMemory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx2')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx3')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx1']))
    assert set(['http://example.org/ctx2', 'http://example.org/ctx3']) == set(bds.contexts())


def test_empty_contexts_with_excludes():
    iom = IOMemory()
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set([]) == set(bds.contexts())


def test_empty_contexts_without_excludes():
    iom = IOMemory()
    bds = BundleDependencyStore(iom)
    assert set([]) == set(bds.contexts())
