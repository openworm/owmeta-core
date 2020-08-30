from __future__ import print_function
import json
import rdflib
from rdflib.term import URIRef
from os.path import join as p
import os
import re
import transaction
from pytest import mark

from owmeta_core.command import OWM
from owmeta_core.context import Context, IMPORTS_CONTEXT_KEY, DEFAULT_CONTEXT_KEY
from owmeta_core.context_common import CONTEXT_IMPORTS
from owmeta_core.data_trans.local_file_ds import LocalFileDataSource as LFDS
from owmeta_core.datasource import DataTranslator, DataSource
from owmeta_core.bundle import Descriptor

from .test_modules.owmclitest01 import DT2
from .TestUtilities import assertRegexpMatches, assertNotRegexpMatches

pytestmark = mark.owm_cli_test


def test_save_diff(owm_project):
    ''' Change something and make a diff '''
    modpath = p(owm_project.testdir, 'test_module')
    os.mkdir(modpath)
    open(p(modpath, '__init__.py'), 'w').close()
    owm_project.writefile(p(modpath, 'command_test_save.py'),
            'tests/test_modules/owmclitest02_command_test_save.py')

    owm_project.writefile(p(modpath, 'monkey.py'),
            'tests/test_modules/owmclitest03_monkey.py')
    print(owm_project.sh('owm save test_module.command_test_save'))
    assertRegexpMatches(owm_project.sh('owm diff'), r'<[^>]+>')


def test_no_write_dependency_on_commit(custom_bundle, owm_project):
    '''
    Make sure we don't have duplicates in the graphs index when we have a dependency

    (This is one of *many* ways in which we're making sure we don't persist data from
     dependencies in the project's store)
    '''

    graph = rdflib.ConjunctiveGraph()
    depctx = 'http://example.org/dep'
    ctxgraph = graph.get_context(URIRef(depctx))
    ctxgraph.add((URIRef('http://ex.org/s'), URIRef('http://ex.org/p'), URIRef('http://ex.org/o'),))
    dep_desc = Descriptor('dep', version=1, includes=(depctx,))

    with custom_bundle(dep_desc, graph=graph, homedir=owm_project.test_homedir):
        owm = owm_project.owm()
        deps = [{'id': 'dep', 'version': 1}]
        owm.config.set('dependencies', json.dumps(deps))

        commit_output = owm_project.sh("owm commit -m 'Commit message'")
        print('COMMIT OUTPUT')
        print(commit_output)

        with open(p(owm_project.testdir, '.owm', 'graphs', 'index')) as f:
            assert list(f.readlines()) == []


def test_save_classes(owm_project):
    modpath = p(owm_project.testdir, 'test_module')
    os.mkdir(modpath)
    open(p(modpath, '__init__.py'), 'w').close()
    owm_project.writefile(p(modpath, 'monkey.py'),
            'tests/test_modules/owmclitest03_monkey.py')
    print(owm_project.sh('owm save test_module.monkey'))
    assertRegexpMatches(owm_project.sh('owm diff'), r'<[^>]+>')


def test_diff_new_context_named(owm_project):
    ''' Test that if we add a new context that its name appears in the diff '''
    modpath = owm_project.make_module('test_module')

    owm_project.writefile(p(modpath, 'monkey.py'),
            'tests/test_modules/owmclitest03_monkey.py')
    owm_project.sh('owm save test_module.monkey')
    assertRegexpMatches(owm_project.sh('owm diff'), r'b http://example.org/primate/monkey')


def test_save_imports(owm_project):
    modpath = owm_project.make_module('test_module')
    owm_project.writefile(p(modpath, 'monkey.py'),
            'tests/test_modules/owmclitest04_monkey_giraffe.py')

    print(owm_project.sh('owm save test_module.monkey'))
    with OWM(owmdir=p(owm_project.testdir, '.owm')).connect() as conn:
        ctx = Context(ident=conn.conf[IMPORTS_CONTEXT_KEY], conf=conn.conf)
        trips = set(ctx.stored.rdf_graph().triples((None, None, None)))
        assert (URIRef(conn.conf[DEFAULT_CONTEXT_KEY]),
                CONTEXT_IMPORTS,
                URIRef('http://example.org/primate/monkey')) in trips
        assert (URIRef(conn.conf[DEFAULT_CONTEXT_KEY]),
                CONTEXT_IMPORTS,
                URIRef('http://example.org/ungulate/giraffe')) in trips


class DT1(DataTranslator):
    class_context = 'http://example.org/context'
    translator_identifier = URIRef('http://example.org/trans1')

    def translate(self, source):
        pass


def test_translator_list(owm_project):
    expected = URIRef('http://example.org/trans1')
    with OWM(owmdir=p(owm_project.testdir, '.owm')).connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = conn(Context)(ident='http://example.org/context')
            conn.mapper.process_class(DT1)

            DT1.definition_context.save(conn.rdf)
            conn.mapper.declare_python_class_registry_entry(DT1, DataTranslator)
            # Create a translator
            ctx(DT1)()

            ctx_id = conn.conf[DEFAULT_CONTEXT_KEY]
            main_ctx = conn(Context)(ident=ctx_id)
            main_ctx.add_import(ctx)
            main_ctx.save_imports()
            ctx.save()
            conn.mapper.save()

    # List translators
    assertRegexpMatches(
        owm_project.sh('owm -o table translator list'),
        re.compile(expected.n3(), flags=re.MULTILINE)
    )


def test_translate_data_source_loader(owm_project):
    owm = owm_project.owm()
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = conn(Context)(ident='http://example.org/context')
            ctx(LFDS)(
                ident='http://example.org/lfds',
                file_name='DSFile',
            )
            ctx.mapper.process_class(DT2)
            ctx(DT2)()
            # Create a translator
            ctx_id = conn.conf[DEFAULT_CONTEXT_KEY]
            print(conn.rdf.serialize(format='nquads').decode('utf-8'))
            print("-------------------------")
            print("DT2.definition_context",
                  DT2.definition_context, id(DT2.definition_context))

            DT2.definition_context.save(conn.rdf)
            print(conn.rdf.serialize(format='nquads').decode('utf-8'))
            print("-------------------------")
            owm.save(DataSource.__module__)
            owm.save(LFDS.__module__)
            owm.save(DT2.__module__)
            LFDS.definition_context.save(conn.rdf)
            main_ctx = conn(Context)(ident=ctx_id)
            main_ctx.add_import(ctx)
            main_ctx.add_import(LFDS.definition_context)
            main_ctx.save_imports()
            ctx.save()
            conn.mapper.save()
            print(conn.rdf.serialize(format='nquads').decode('utf-8'))
    owm_project.make_module('tests')
    modpath = owm_project.copy('tests/test_modules', 'tests/test_modules')
    dsfile = owm_project.writefile('DSFile', 'some stuff')
    owm_project.writefile(p(modpath, 'OWMCLITest.py'),
        'tests/test_modules/owmclitest01.py')

    # Do translation
    assertRegexpMatches(
        owm_project.sh('owm translate http://example.org/trans1 http://example.org/lfds'),
        re.escape(dsfile)
    )


@mark.core_bundle_version(1)
@mark.core_bundle
def test_source_list(owm_project, core_bundle):
    owm = owm_project.owm()
    deps = [{'id': 'openworm/owmeta-core', 'version': 1}]
    owm.config.set('dependencies', json.dumps(deps))

    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = conn(Context)(ident='http://example.org/context')
            ctx(LFDS)(
                ident='http://example.org/lfds',
                file_name='DSFile',
            )

            ctx.add_import(LFDS.definition_context)
            ctx.save_imports()
            defctx = conn(Context)(ident=owm_project.default_context_id)
            defctx.add_import(ctx)
            defctx.save_imports()

            ctx.save()
            conn.mapper.save()

    assertRegexpMatches(owm_project.sh('owm source list'),
            '<http://example.org/lfds>')


def test_registry_list(owm_project):
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_monkey')
    print("MONKEY")
    print(save_out)
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_donkey')
    print("DONKEY")
    print(save_out)
    registry_list_out = owm_project.sh('owm -o json registry list')
    assertRegexpMatches(registry_list_out, 'tests.test_modules.owmclitest05_monkey')
    assertRegexpMatches(registry_list_out, 'tests.test_modules.owmclitest05_donkey')


def test_registry_list_module_filter(owm_project):
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_monkey')
    print("MONKEY")
    print(save_out)
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_donkey')
    print("DONKEY")
    print(save_out)
    registry_list_out = owm_project.sh('owm -o json registry list --module tests.test_modules.owmclitest05_monkey')
    assertNotRegexpMatches(registry_list_out, 'tests.test_modules.owmclitest05_donkey')
