'''
Utilies for graph serialization
'''
import hashlib
from os.path import join as p, exists

from rdflib import plugin
from rdflib.parser import Parser, create_input_source
from rdflib.serializer import Serializer

from .rdf_utils import BatchAddGraph


def write_canonical_to_file(graph, file_name):
    '''
    Write a graph to a file such that the contents would only differ if the
    set of triples in the graph were different. The serialization format is
    N-Triples.

    Parameters
    ----------
    graph : rdflib.graph.Graph
        The graph to write
    file_name : str
        The name of the file to write to
    '''
    with open(file_name, 'wb') as f:
        write_canonical(graph, f)


def write_canonical(graph, out):
    serializer = plugin.get('nt', Serializer)(sorted(graph))
    serializer.serialize(out)


def read_canonical_from_file(ctx, dest, graph_fname):
    bag = BatchAddGraph(dest, batchsize=10000)
    parser = plugin.get('nt', Parser)()
    with open(graph_fname, 'rb') as f, bag.get_context(ctx) as g:
        parser.parse(create_input_source(f), g)


def gen_ctx_fname(ident, base_directory, hashfunc=None):
    hs = (hashfunc or hashlib.sha256)(ident.encode('UTF-8')).hexdigest()
    fname = p(base_directory, hs + '.nt')
    i = 1
    while exists(fname):
        fname = p(base_directory, hs + '-' + str(i) + '.nt')
        i += 1
    return fname
