from collections import namedtuple
import json
from os import listdir, makedirs
from os.path import join as p, isdir, isfile
import transaction
from unittest.mock import patch
from tempfile import TemporaryDirectory

import pytest
import rdflib
from rdflib.term import URIRef

from owmeta_core.bundle import (Installer, Descriptor, make_include_func, FilesDescriptor,
                                UncoveredImports, DependencyDescriptor, TargetIsNotEmpty,
                                Remote, Bundle, BUNDLE_MANIFEST_FILE_NAME)
from owmeta_core.context import IMPORTS_CONTEXT_KEY, CLASS_REGISTRY_CONTEXT_KEY
from owmeta_core.context_common import CONTEXT_IMPORTS


Dirs = namedtuple('Dirs', ('source_directory', 'bundles_directory'))


@pytest.fixture
def dirs():
    with TemporaryDirectory() as source_directory,\
            TemporaryDirectory() as bundles_directory:
        yield Dirs(source_directory, bundles_directory)


def test_bundle_install_directory(dirs):
    d = Descriptor('test')
    bi = Installer(*dirs, graph=rdflib.ConjunctiveGraph())
    bi.install(d)
    assert isdir(p(dirs.bundles_directory, 'test', '1'))


def test_context_hash_file_exists(dirs):
    d = Descriptor('test')
    ctxid = 'http://example.org/ctx1'
    d.includes.add(make_include_func(ctxid))
    g = rdflib.ConjunctiveGraph()
    cg = g.get_context(ctxid)
    cg.add((aURI('a'), aURI('b'), aURI('c')))
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    assert isfile(p(dirs.bundles_directory, 'test', '1', 'graphs', 'hashes'))


def test_context_index_file_exists(dirs):
    d = Descriptor('test')
    ctxid = 'http://example.org/ctx1'
    d.includes.add(make_include_func(ctxid))
    g = rdflib.ConjunctiveGraph()
    cg = g.get_context(ctxid)
    cg.add((aURI('a'), aURI('b'), aURI('c')))
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    assert isfile(p(dirs.bundles_directory, 'test', '1', 'graphs', 'index'))


def test_context_hash_file_contains_ctxid(dirs):
    d = Descriptor('test')
    ctxid = 'http://example.org/ctx1'
    d.includes.add(make_include_func(ctxid))
    g = rdflib.ConjunctiveGraph()
    cg = g.get_context(ctxid)
    with transaction.manager:
        cg.add((aURI('a'), aURI('b'), aURI('c')))
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    with open(p(dirs.bundles_directory, 'test', '1', 'graphs', 'hashes'), 'rb') as f:
        assert f.read().startswith(ctxid.encode('UTF-8'))


def test_context_index_file_contains_ctxid(dirs):
    d = Descriptor('test')
    ctxid = 'http://example.org/ctx1'
    d.includes.add(make_include_func(ctxid))
    g = rdflib.ConjunctiveGraph()
    cg = g.get_context(ctxid)
    with transaction.manager:
        cg.add((aURI('a'), aURI('b'), aURI('c')))
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    with open(p(dirs.bundles_directory, 'test', '1', 'graphs', 'index'), 'rb') as f:
        assert f.read().startswith(ctxid.encode('UTF-8'))


def test_multiple_context_hash(dirs):
    d = Descriptor('test')
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(ctxid_2))
    g = rdflib.ConjunctiveGraph()
    cg = g.get_context(ctxid_1)
    with transaction.manager:
        cg.add((aURI('a'), aURI('b'), aURI('c')))

    cg = g.get_context(ctxid_2)
    with transaction.manager:
        cg.add((aURI('a'), aURI('b'), aURI('c')))

    bi = Installer(*dirs, graph=g)
    bi.install(d)
    with open(p(dirs.bundles_directory, 'test', '1', 'graphs', 'hashes'), 'rb') as f:
        contents = f.read()
        assert ctxid_1.encode('UTF-8') in contents
        assert ctxid_2.encode('UTF-8') in contents


def test_no_dupe(dirs):
    '''
    Test that if we have two contexts with the same contents that we don't create more
    than one file for it.

    The index will point to the same file for the two contexts
    '''
    d = Descriptor('test')
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(ctxid_2))
    g = rdflib.ConjunctiveGraph()
    cg = g.get_context(ctxid_1)
    with transaction.manager:
        cg.add((aURI('a'), aURI('b'), aURI('c')))

    cg = g.get_context(ctxid_2)
    with transaction.manager:
        cg.add((aURI('a'), aURI('b'), aURI('c')))

    bi = Installer(*dirs, graph=g)
    bi.install(d)

    graph_files = [x for x in listdir(p(dirs.bundles_directory, 'test', '1', 'graphs')) if x.endswith('.nt')]
    assert len(graph_files) == 1


def test_file_copy(dirs):
    d = Descriptor('test')
    open(p(dirs[0], 'somefile'), 'w').close()
    d.files = FilesDescriptor()
    d.files.includes.add('somefile')
    g = rdflib.ConjunctiveGraph()
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    bfiles = p(dirs.bundles_directory, 'test', '1', 'files')
    assert set(listdir(bfiles)) == set(['hashes', 'somefile'])


def test_file_pattern_copy(dirs):
    d = Descriptor('test')
    open(p(dirs[0], 'somefile'), 'w').close()
    d.files = FilesDescriptor()
    d.files.patterns.add('some*')
    g = rdflib.ConjunctiveGraph()
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    bfiles = p(dirs.bundles_directory, 'test', '1', 'files')
    assert set(listdir(bfiles)) == set(['hashes', 'somefile'])


def test_file_hash(dirs):
    d = Descriptor('test')
    open(p(dirs[0], 'somefile'), 'w').close()
    d.files = FilesDescriptor()
    d.files.includes.add('somefile')
    g = rdflib.ConjunctiveGraph()
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    assert isfile(p(dirs.bundles_directory, 'test', '1', 'files', 'hashes'))


def test_file_hash_content(dirs):
    d = Descriptor('test')
    open(p(dirs[0], 'somefile'), 'w').close()
    d.files = FilesDescriptor()
    d.files.includes.add('somefile')
    g = rdflib.ConjunctiveGraph()
    bi = Installer(*dirs, graph=g)
    bi.install(d)
    with open(p(dirs.bundles_directory, 'test', '1', 'files', 'hashes'), 'rb') as f:
        contents = f.read()
        assert b'somefile' in contents


def test_uncovered_imports(dirs):
    '''
    If we have imports and no dependencies, then thrown an exception if we have not
    included them in the bundle
    '''
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)
    cg_imp = g.get_context(imports_ctxid)
    with transaction.manager:
        cg_1.add((aURI('a'), aURI('b'), aURI('c')))
        cg_2.add((aURI('d'), aURI('e'), aURI('f')))
        cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g)
    with pytest.raises(UncoveredImports):
        bi.install(d)


def test_imports_are_included(dirs):
    '''
    If we have imports and no dependencies, then thrown an exception if we have not
    included them in the bundle
    '''
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(ctxid_2))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)
    cg_imp = g.get_context(imports_ctxid)
    with transaction.manager:
        cg_1.add((aURI('a'), aURI('b'), aURI('c')))
        cg_2.add((aURI('d'), aURI('e'), aURI('f')))
        cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g)
    bi.install(d)
    with Bundle(d.id, dirs.bundles_directory) as bnd:
        g = bnd.rdf.get_context(bnd.conf[IMPORTS_CONTEXT_KEY])
        assert (URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)) in g


def test_unrelated_imports_excluded(dirs):
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'
    ctxid_3 = 'http://example.org/ctx3'
    ctxid_4 = 'http://example.org/ctx4'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(ctxid_2))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)
    cg_3 = g.get_context(ctxid_3)
    cg_4 = g.get_context(ctxid_4)
    cg_imp = g.get_context(imports_ctxid)
    with transaction.manager:
        cg_1.add((aURI('a'), aURI('b'), aURI('c')))
        cg_2.add((aURI('d'), aURI('e'), aURI('f')))
        cg_3.add((aURI('g'), aURI('h'), aURI('i')))
        cg_4.add((aURI('j'), aURI('k'), aURI('l')))
        cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))
        cg_imp.add((URIRef(ctxid_3), CONTEXT_IMPORTS, URIRef(ctxid_4)))

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g)
    bi.install(d)
    with Bundle(d.id, dirs.bundles_directory) as bnd:
        g = bnd.rdf.get_context(bnd.conf[IMPORTS_CONTEXT_KEY])
        assert (URIRef(ctxid_3), CONTEXT_IMPORTS, URIRef(ctxid_4)) not in g


def test_imports_in_dependencies(dirs):
    '''
    If we have imports and a dependency includes the context, then we shouldn't have an
    error.

    Versioned bundles are assumed to be immutable, so we won't re-fetch a bundle already
    in the local index
    '''
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(imports_ctxid))
    d.dependencies.add(DependencyDescriptor('dep'))

    dep_d = Descriptor('dep')
    dep_d.includes.add(make_include_func(ctxid_2))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)
    cg_imp = g.get_context(imports_ctxid)
    with transaction.manager:
        cg_1.add((aURI('a'), aURI('b'), aURI('c')))
        cg_2.add((aURI('d'), aURI('e'), aURI('f')))
        cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g)
    bi.install(dep_d)
    bi.install(d)


def test_imports_in_unfetched_dependencies(dirs):
    '''
    If we have imports and a dependency includes the context, then we shouldn't have an
    error.

    Versioned bundles are assumed to be immutable, so we won't re-fetch a bundle already
    in the local index
    '''
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(imports_ctxid))
    d.dependencies.add(DependencyDescriptor('dep'))

    dep_d = Descriptor('dep')
    dep_d.includes.add(make_include_func(ctxid_2))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)
    cg_imp = g.get_context(imports_ctxid)

    cg_1.add((URIRef('http://example.com/a'), URIRef('http://example.com/b'), URIRef('http://example.com/c')))
    cg_2.add((URIRef('http://example.com/d'), URIRef('http://example.com/e'), URIRef('http://example.com/f')))
    cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))

    class loader_class(object):
        def __init__(self, *args):
            self.bi = None

        def can_load(self, *args):
            return True

        def can_load_from(self, *args):
            return True

        def bundle_versions(self, *args):
            return [1]

        def __call__(self, *args):
            self.bi.install(dep_d)

    loader = loader_class()

    class remote_class(Remote):
        def generate_loaders(self, *args):
            yield loader

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g,
            remotes=[remote_class('remote')])
    loader.bi = bi

    with patch('owmeta_core.bundle.LOADER_CLASSES', (loader_class,)):
        bi.install(d)


def test_imports_in_transitive_dependency_not_included(dirs):
    '''
    If we have imports and a transitive dependency includes the context, then we should
    still have an error.

    Versioned bundles are assumed to be immutable, so we won't re-fetch a bundle already
    in the local index
    '''
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))
    d.includes.add(make_include_func(imports_ctxid))
    d.dependencies.add(DependencyDescriptor('dep'))

    dep_d = Descriptor('dep')
    dep_d.dependencies.add(DependencyDescriptor('dep_dep'))

    dep_dep_d = Descriptor('dep_dep')
    dep_dep_d.includes.add(make_include_func(ctxid_2))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)
    cg_imp = g.get_context(imports_ctxid)
    cg_1.add((aURI('a'), aURI('b'), aURI('c')))
    cg_2.add((aURI('d'), aURI('e'), aURI('f')))
    cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g)
    bi.install(dep_dep_d)
    bi.install(dep_d)
    with pytest.raises(UncoveredImports):
        bi.install(d)


def test_empty_context_uncovered_imports(dirs):
    '''
    If we have imports and no dependencies, then thrown an exception if we have not
    included them in the bundle
    '''
    imports_ctxid = 'http://example.org/imports'
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.empties.add(ctxid_1)

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_imp = g.get_context(imports_ctxid)
    with transaction.manager:
        cg_imp.add((URIRef(ctxid_1), CONTEXT_IMPORTS, URIRef(ctxid_2)))

    bi = Installer(*dirs, imports_ctx=imports_ctxid, graph=g)
    with pytest.raises(UncoveredImports):
        bi.install(d)


def test_class_registry_in_manifest(dirs):
    '''
    If a class registry context is specified, then include it
    '''
    cr_ctxid = 'http://example.org/class_registry'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()

    bi = Installer(*dirs, class_registry_ctx=cr_ctxid, graph=g)
    bdir = bi.install(d)
    with open(p(bdir, BUNDLE_MANIFEST_FILE_NAME)) as mf:
        manifest_data = json.load(mf)
        assert manifest_data[CLASS_REGISTRY_CONTEXT_KEY]


def test_class_registry_contents(dirs):
    '''
    If a class registry context is specified, then include it
    '''
    cr_ctxid = 'http://example.org/class_registry'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()
    cg_cr = g.get_context(cr_ctxid)
    with transaction.manager:
        cg_cr.add((aURI('blah'), aURI('bruh'), aURI('uhhhh')))

    bi = Installer(*dirs, class_registry_ctx=cr_ctxid, graph=g)
    bi.install(d)

    with Bundle(d.id, dirs.bundles_directory) as bnd:
        g = bnd.rdf.get_context(bnd.conf[CLASS_REGISTRY_CONTEXT_KEY])
        assert (aURI('blah'), aURI('bruh'), aURI('uhhhh')) in g


def test_fail_on_non_empty_target(dirs):
    d = Descriptor('test')
    g = rdflib.ConjunctiveGraph()
    bi = Installer(*dirs, graph=g)
    bundles_directory = dirs[1]
    sma = p(bundles_directory, 'test', '1', 'blah')
    makedirs(sma)
    with pytest.raises(TargetIsNotEmpty):
        bi.install(d)


def test_dependency_version_in_manifest_without_spec(dirs):
    '''
    It is permitted to not specify the version of a bundle dependency in the descriptor,
    but we must pin a specific version of the bundle in the manifest.
    '''
    ctxid_1 = 'http://example.org/ctx1'
    ctxid_2 = 'http://example.org/ctx2'

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(ctxid_1))
    d.dependencies.add(DependencyDescriptor('dep'))

    dep_d = Descriptor('dep')
    dep_d.includes.add(make_include_func(ctxid_2))

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()

    cg_1 = g.get_context(ctxid_1)
    cg_2 = g.get_context(ctxid_2)

    cg_1.add((aURI('a'), aURI('b'), aURI('c')))
    cg_2.add((aURI('d'), aURI('e'), aURI('f')))

    bi = Installer(*dirs, graph=g)
    bi.install(dep_d)
    bi.install(d)
    test_bnd = Bundle('test', bundles_directory=dirs.bundles_directory)
    assert test_bnd.manifest_data['dependencies'][0]['version'] == 1


def aURI(c):
    return URIRef(f'http://example.org/uri#{c}')
