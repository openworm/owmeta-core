from contextlib import contextmanager
import hashlib
import json
import logging
import os
from os.path import join as p
import tempfile

from owmeta_core.bundle import Descriptor, Installer
from owmeta_core.bundle.archive import Archiver
from owmeta_core.context import Context
from owmeta_pytest_plugin import bundle_fixture_helper
from pytest import fixture
from rdflib.term import URIRef
from rdflib.graph import ConjunctiveGraph


L = logging.getLogger(__name__)

os.environ['HTTPS_PYTEST_FIXTURES_CERT'] = p('tests', 'cert.pem')
os.environ['HTTPS_PYTEST_FIXTURES_KEY'] = p('tests', 'key.pem')


@fixture
def tempdir():
    with tempfile.TemporaryDirectory(prefix=__name__ + '.') as td:
        yield td


@fixture
def http_bundle_server(http_server):
    with open(p('tests', 'test_data', 'example_bundle.tar.xz'), 'rb') as f:
        bundle_data = f.read()
        bundle_hash = hashlib.sha224(bundle_data).hexdigest()

    def handler(server_data):
        class _Handler(server_data.basic_handler):
            def do_GET(self):
                self.queue_reuqest()
                if self.path == '/index.json':
                    self.send_response(200)
                    self.send_header('ETag', 'doesntmatter')
                    self.send_header('Cache-Control', 'max-age=6000')
                    self.end_headers()
                    host, port = self.server.server_address
                    index_data = json.dumps({"example/aBundle": {
                        "23": {"url": f"http://{host}:{port}/bundle",
                            "hashes": {"sha224": bundle_hash}}}})
                    self.wfile.write(index_data.encode())
                else:
                    self.send_response(200)
                    self.send_header('ETag', 'whocares')
                    self.send_header('Cache-Control', 'max-age=6000')
                    self.end_headers()
                    self.wfile.write(bundle_data)
        return _Handler

    http_server.make_server(handler)
    http_server.restart()
    yield http_server


core_bundle_1 = fixture(bundle_fixture_helper('openworm/owmeta-core', 1))
core_bundle = fixture(bundle_fixture_helper('openworm/owmeta-core'))


@fixture
def test_bundle():
    with bundle_helper(Descriptor('test')) as data:
        yield data


@fixture
def bundle_archive():
    with bundle_archive_helper(Descriptor('test')) as data:
        yield data


@fixture
def custom_bundle_archive():
    yield bundle_archive_helper


@fixture
def custom_bundle():
    yield bundle_helper


@contextmanager
def bundle_helper(descriptor, graph=None, bundles_directory=None, homedir=None, **kwargs):
    '''
    Helper for creating bundles for testing.

    Uses `~owmeta_core.bundle.Installer` to lay out a bundle

    Parameters
    ----------
    descriptor : Descriptor
        Describes the bundle
    graph : rdflib.graph.ConjunctiveGraph, optional
        Graph from which the bundle contexts will be generated. If not provided, a graph
        will be created with the triple ``(ex:a, ex:b, ex:c)`` in a context named ``ex:ctx``,
        where ``ex:`` expands to ``http://example.org/``
    bundles_directory : str, optional
        The directory where the bundles should be installed. If not provided, creates a
        temporary directory to house the bundles and cleans them up afterwards
    homedir : str, optional
        Test home directory. If not provided, one will be created based on test directory
    '''
    class BundleData(object):
        pass

    res = BundleData()
    with tempfile.TemporaryDirectory(prefix=__name__ + '.') as testdir:
        res.testdir = testdir
        res.test_homedir = homedir or p(res.testdir, 'homedir')
        res.bundle_source_directory = p(res.testdir, 'bundle_source')
        res.bundles_directory = bundles_directory or p(res.test_homedir, '.owmeta', 'bundles')
        if not homedir:
            os.mkdir(res.test_homedir)
        os.mkdir(res.bundle_source_directory)
        if not bundles_directory:
            os.makedirs(res.bundles_directory)

        # This is a bit of an integration test since it would be a PITA to maintain the bundle
        # format separately from the installer
        res.descriptor = descriptor
        if graph is None:
            graph = ConjunctiveGraph()
            ctxg = graph.get_context(URIRef('http://example.org/ctx'))
            ctxg.add((URIRef('http://example.org/a'),
                      URIRef('http://example.org/b'),
                      URIRef('http://example.org/c')))
        res.installer = Installer(res.bundle_source_directory,
                                  res.bundles_directory,
                                  graph=graph,
                                  **kwargs)
        res.bundle_directory = res.installer.install(res.descriptor)
        yield res


@contextmanager
def bundle_archive_helper(*args, pre_pack_callback=None, **kwargs):
    with bundle_helper(*args, **kwargs) as bundle_data:
        if pre_pack_callback:
            pre_pack_callback(bundle_data)
        bundle_data.archive_path = Archiver(bundle_data.testdir).pack(
                bundle_directory=bundle_data.bundle_directory,
                target_file_name='bundle.tar.xz')
        yield bundle_data


@fixture
def context():
    return Context('http://example.org/test-context')
