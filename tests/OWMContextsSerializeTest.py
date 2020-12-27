from os.path import join as p, isfile
import subprocess

import pytest
from pytest import mark
from rdflib.term import URIRef
from .TestUtilities import assertRegexpMatches

pytestmark = mark.owm_cli_test


def test_serialize(owm_project):
    with owm_project.owm().connect() as owm:
        ctx1 = owm.rdf.graph('http://example.org/ctx1')
        ctx1.add((URIRef('http://example.org/s1'), URIRef('http://example.org/p1'), URIRef('http://example.org/o1')))
        ctx1.add((URIRef('http://example.org/s2'), URIRef('http://example.org/p2'), URIRef('http://example.org/o2')))

        ctx2 = owm.rdf.graph('http://example.org/ctx2')
        ctx2.add((URIRef('http://example.org/s3'), URIRef('http://example.org/p3'), URIRef('http://example.org/o3')))
        ctx2.add((URIRef('http://example.org/s4'), URIRef('http://example.org/p4'), URIRef('http://example.org/o4')))

    output = owm_project.sh('owm contexts serialize http://example.org/ctx1')
    assert output == '''\
<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> <http://example.org/ctx1> .
<http://example.org/s2> <http://example.org/p2> <http://example.org/o2> <http://example.org/ctx1> .


'''


def test_serialize_empty_default_graph(owm_project):
    with owm_project.owm().connect() as owm:
        ctx1 = owm.rdf.graph('http://example.org/ctx1')
        ctx1.add((URIRef('http://example.org/s1'), URIRef('http://example.org/p1'), URIRef('http://example.org/o1')))
        ctx1.add((URIRef('http://example.org/s2'), URIRef('http://example.org/p2'), URIRef('http://example.org/o2')))

        ctx2 = owm.rdf.graph('http://example.org/ctx2')
        ctx2.add((URIRef('http://example.org/s3'), URIRef('http://example.org/p3'), URIRef('http://example.org/o3')))
        ctx2.add((URIRef('http://example.org/s4'), URIRef('http://example.org/p4'), URIRef('http://example.org/o4')))

    output = owm_project.sh('owm contexts serialize -f n3')
    assert output == '\n\n'


def test_serialize_whole_graph(owm_project):
    with owm_project.owm().connect() as owm:
        ctx1 = owm.rdf.graph('http://example.org/ctx1')
        ctx1.add((URIRef('http://example.org/s1'), URIRef('http://example.org/p1'), URIRef('http://example.org/o1')))
        ctx1.add((URIRef('http://example.org/s2'), URIRef('http://example.org/p2'), URIRef('http://example.org/o2')))

        ctx2 = owm.rdf.graph('http://example.org/ctx2')
        ctx2.add((URIRef('http://example.org/s3'), URIRef('http://example.org/p3'), URIRef('http://example.org/o3')))
        ctx2.add((URIRef('http://example.org/s4'), URIRef('http://example.org/p4'), URIRef('http://example.org/o4')))

    output = owm_project.sh('owm contexts serialize -f n3 --whole-graph')
    assert output == '''\
@prefix ns1: <http://example.org/> .

ns1:s1 ns1:p1 ns1:o1 .

ns1:s2 ns1:p2 ns1:o2 .

ns1:s3 ns1:p3 ns1:o3 .

ns1:s4 ns1:p4 ns1:o4 .


'''


def test_serialize_whole_graph_with_context_error(owm_project):
    with pytest.raises(subprocess.CalledProcessError) as raised:
        owm_project.sh('owm contexts serialize -f n3 --whole-graph'
                ' http://example.org/context', stderr=subprocess.STDOUT)
    print(dir(raised), type(raised), raised.value)
    assertRegexpMatches(raised.value.output.decode('UTF-8'),
            r'whole.graph.*context|context.*whole.graph')


def test_serialize_to_file(owm_project):
    with owm_project.owm().connect() as owm:
        ctx1 = owm.rdf.graph('http://example.org/ctx1')
        ctx1.add((URIRef('http://example.org/s1'), URIRef('http://example.org/p1'), URIRef('http://example.org/o1')))
        ctx1.add((URIRef('http://example.org/s2'), URIRef('http://example.org/p2'), URIRef('http://example.org/o2')))

    output_file = p(owm_project.testdir, "f.n4")
    owm_project.sh(f'owm contexts serialize -w {output_file} http://example.org/ctx1')
    assert isfile(output_file)
