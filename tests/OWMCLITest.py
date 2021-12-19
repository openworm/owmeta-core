from __future__ import print_function
import json
import rdflib
from rdflib.term import URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from os.path import join as p
import os
import re

import transaction
from pytest import mark, fixture, raises

from owmeta_core import BASE_CONTEXT
from owmeta_core.command import OWM
from owmeta_core.context import Context, IMPORTS_CONTEXT_KEY, DEFAULT_CONTEXT_KEY
from owmeta_core.context_common import CONTEXT_IMPORTS
from owmeta_core.data_trans.local_file_ds import LocalFileDataSource as LFDS
from owmeta_core.dataobject import DataObject
from owmeta_core.datasource import DataTranslator, DataSource, Transformation, Translation
from owmeta_core.bundle import Descriptor
from owmeta_pytest_plugin import bundle_versions

from .test_modules.owmclitest01 import DT2
from .TestUtilities import assertRegexpMatches, assertNotRegexpMatches

pytestmark = mark.owm_cli_test


EX = rdflib.Namespace('http://example.org/')


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
    with owm_project.owm().connect() as conn, transaction.manager:
        ctx = conn(Context)(ident='http://example.org/context')
        conn.mapper.process_class(DT1)

        DT1.definition_context.save(conn.rdf)
        conn.mapper.declare_python_class_registry_entry(DT1, DataTranslator)
        # Create a translator
        ctx(DT1)(ident=expected)

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


@bundle_versions('core_bundle', [1, 2])
def test_translator_list_kinds(owm_project, core_bundle):
    owm_project.fetch(core_bundle)
    owm = owm_project.owm()
    # TODO: Fix this so we use the correct version of the core bundle
    deps = [{'id': 'openworm/owmeta-core', 'version': 1}]
    owm.config.set('dependencies', json.dumps(deps))

    with owm.connect() as conn, transaction.manager:
        defctx = conn(Context)(ident=owm_project.default_context_id)
        defctx.add_import(BASE_CONTEXT)
        defctx.save_imports()

    output = owm_project.sh('owm translator list-kinds').strip().split('\n')
    assert set(output) == set(['<http://schema.openworm.org/2020/07/CSVDataTranslator>'])


def test_translator_show(owm_project):
    trans_id = URIRef('http://example.org/trans1')
    with owm_project.owm().connect() as conn, transaction.manager:
        # Create data Translator
        ctx = conn(Context)(ident='http://example.org/context')
        conn.mapper.process_class(DT1)

        DT1.definition_context.save(conn.rdf)
        conn.mapper.declare_python_class_registry_entry(DT1, DataTranslator)
        # Create a translator
        dt1 = ctx(DT1)(ident=trans_id)

        ctx_id = conn.conf[DEFAULT_CONTEXT_KEY]
        main_ctx = conn(Context)(ident=ctx_id)
        main_ctx.add_import(ctx)
        main_ctx.save_imports()
        ctx.save()
        conn.mapper.save()

    assert str(dt1) == owm_project.sh(f'owm translator show {trans_id}').strip()


def test_translator_create(owm_project):
    with owm_project.owm().connect() as conn, transaction.manager:
        conn.mapper.process_class(DT1)

        DT1.definition_context.save(conn.rdf)
        conn.mapper.declare_python_class_registry_entry(DT1, DataTranslator)
        conn.mapper.save()

    ident = owm_project.sh(f'owm translator create {DT1.rdf_type}').strip()

    with owm_project.owm().connect() as conn:
        assert (rdflib.URIRef(ident), rdflib.RDF.type, DT1.rdf_type) in conn.rdf


def test_translator_rm(owm_project):
    trans_id = URIRef('http://example.org/trans1')
    with owm_project.owm().connect() as conn, transaction.manager:
        # Create data Translator
        ctx = conn(Context)(ident='http://example.org/context')
        conn.mapper.process_class(DT1)

        DT1.definition_context.save(conn.rdf)
        conn.mapper.declare_python_class_registry_entry(DT1, DataTranslator)
        # Create a translator
        ctx(DT1)(ident=trans_id)

        ctx_id = conn.conf[DEFAULT_CONTEXT_KEY]
        main_ctx = conn(Context)(ident=ctx_id)
        main_ctx.add_import(ctx)
        main_ctx.save_imports()
        ctx.save()
        conn.mapper.save()

    owm_project.sh(f'owm translator rm {trans_id}')

    with owm_project.owm().connect() as conn:
        assert (trans_id, None, None) not in conn.rdf


@fixture
def lfds_with_file(owm_project):
    owm = owm_project.owm()

    class Namespace(object):
        file_name = 'DSfile'
        ident = 'http://example.org/lfds'
        file_contents = 'some stuff'

    res = Namespace()
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = conn(Context)(ident='http://example.org/context')
            ctx(LFDS)(
                ident=res.ident,
                file_name=res.file_name,
            )

            owm.save(DataSource.__module__)
            owm.save(LFDS.__module__)
            LFDS.definition_context.save(conn.rdf)
            main_ctx = owm.default_context
            main_ctx.add_import(ctx)
            main_ctx.add_import(LFDS.definition_context)
            main_ctx.save_imports()
            ctx.save()
            conn.mapper.save()
    # We're relying on the WorkingDirectoryProvider to be able to load this file for the
    # datasource defined above. writefile writes to the working directory for the `owm`
    # execution below
    owm_project.writefile(res.file_name, res.file_contents)

    return res


def test_translate_data_source_loader(owm_project, lfds_with_file):
    with owm_project.owm().connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = conn(Context)(ident='http://example.org/context')
            ctx.mapper.process_class(DT2)
            ctx(DT2)(ident='http://example.org/trans1')
            # Create a translator

            DT2.definition_context.save(conn.rdf)
            conn.owm.save(DT2.__module__)
            main_ctx = conn.owm.default_context
            main_ctx.add_import(ctx)
            main_ctx.save_imports()
            ctx.save()
            conn.mapper.save()
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')

    out_ds = owm_project.sh(f'owm --full-trace translate http://example.org/trans1 {lfds_with_file.ident}').strip()
    with owm_project.owm().connect() as conn:
        ctx = conn.owm.default_context.stored
        print('ds_id', repr(out_ds), 'default_context', ctx)
        for loaded in ctx(DataSource)(ident=out_ds).load():
            break
        else: # no break
            raise Exception(f'Failed to load datasource for {out_ds}')

        with open(loaded.full_path()) as f:
            assertRegexpMatches(f.read(), rf'^{lfds_with_file.file_contents}$')


def test_translate_table_output(owm_project):
    from .test_modules.owmclitest07_translator import DT, LABEL

    owm = owm_project.owm()

    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = conn(Context)(ident=EX.context)
            insrc = ctx(DataSource)(ident=EX['in'])

            ctx.mapper.process_class(DT)

            DT.definition_context.save(conn.rdf)
            ctx(DT)(ident='http://example.org/trans1')

            owm.save(DataSource.__module__)
            owm.save(DT.__module__)
            main_ctx = owm.default_context
            main_ctx.add_import(ctx)
            main_ctx.add_import(DataSource.definition_context)
            main_ctx.save_imports()
            ctx.save()
            conn.mapper.save()

    owm_project.make_module('tests')
    modpath = p('tests', 'test_modules')
    owm_project.copy(modpath, modpath)
    owm_project.writefile(p(modpath, 'owmclitest07_translator.py'))
    out_ds = owm_project.sh(
            'owm --full-trace -o table --columns ID,source,rdfs_label'
            f' translate {DT.translator_identifier} {insrc.identifier}').strip()
    print(out_ds)
    assert re.search(rf'{EX.out} *{EX["in"]} *{LABEL!r}', out_ds)


@bundle_versions('core_bundle', [1, 2])
def test_source_list(owm_project, core_bundle):
    owm_project.fetch(core_bundle)
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
                rdfs_comment='hello, world'
            )

            ctx.add_import(LFDS.definition_context)
            ctx.save_imports()
            defctx = conn(Context)(ident=owm_project.default_context_id)
            defctx.add_import(ctx)
            defctx.save_imports()

            ctx.save()
            conn.mapper.save()

    assertRegexpMatches(owm_project.sh('owm -o table --columns ID,file_name,rdfs_comment source list'),
            'http://example.org/lfds +\'DSFile\' +\'hello, world\'')


@bundle_versions('core_bundle', [1, 2])
def test_source_list_kinds(owm_project, core_bundle):
    owm_project.fetch(core_bundle)
    owm = owm_project.owm()
    deps = [{'id': 'openworm/owmeta-core', 'version': 1}]
    owm.config.set('dependencies', json.dumps(deps))
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            defctx = conn(Context)(ident=owm_project.default_context_id)
            defctx.add_import(BASE_CONTEXT)
            defctx.save_imports()

    output = owm_project.sh('owm source list-kinds').strip().split('\n')
    assert set(output) == set([
        '<http://schema.openworm.org/2020/07/data_sources/LocalFileDataSource>',
        '<http://schema.openworm.org/2020/07/data_sources/CSVDataSource>',
        '<http://schema.openworm.org/2020/07/data_sources/CSVHTTPFileDataSource>',
        '<http://schema.openworm.org/2020/07/data_sources/XLSXHTTPFileDataSource>',
        '<http://schema.openworm.org/2020/07/data_sources/FileDataSource>',
        '<http://schema.openworm.org/2020/07/data_sources/DataObjectContextDataSource>',
        '<http://schema.openworm.org/2020/07/data_sources/HTTPFileDataSource>'])


def test_source_derivs(owm_project):
    owm = owm_project.owm()
    with owm.connect():
        DS = owm.default_context(DataSource)
        ds0 = DS(key='ds0')
        ds1 = DS(key='ds1')
        ds2 = DS(key='ds2')
        ds1.source(ds0)
        ds2.source(ds1)
        with transaction.manager:
            owm.default_context.save()

    derivs = owm_project.sh(f'owm source derivs {ds0.identifier}')
    assert f'{ds0.identifier} → {ds1.identifier}' in derivs
    assert f'{ds1.identifier} → {ds2.identifier}' in derivs


def test_source_show(owm_project):
    owm = owm_project.owm()
    with owm.connect() as conn:
        DS = owm.default_context(DataSource)
        ds0 = DS(key='ds0')
        owm.default_context.add_import(DataSource.definition_context)
        conn.mapper.process_class(DataSource)
        with transaction.manager:
            owm.default_context.save()
            owm.default_context.save_imports(transitive=False)
            conn(DataSource.definition_context).save()
            conn.mapper.save()
        print(conn.rdf.serialize(format='n3'))
    res = owm_project.sh(f'owm source show {ds0.identifier}')
    assert 'ds0' in res


def test_source_rm_srcs_and_transformations(owm_project):
    owm = owm_project.owm()
    with owm.connect() as conn:
        DS = owm.default_context(DataSource)
        ds0 = DS(key='ds0')
        tf1 = owm.default_context(Transformation)(key='tf1')
        ds0.transformation(tf1)
        owm.default_context.add_import(DataSource.definition_context)
        conn.mapper.process_class(DataSource)
        with transaction.manager:
            owm.default_context.save()
            owm.default_context.save_imports(transitive=False)
            conn(DataSource.definition_context).save()
            conn.mapper.save()
        print(conn.rdf.serialize(format='n3'))
    owm_project.sh(f'owm source rm {ds0.identifier}')
    with owm_project.owm().connect(read_only=True) as conn:
        assert [] == list(conn.rdf.triples((tf1.identifier, None, None)))
        assert [] == list(conn.rdf.triples((ds0.identifier, None, None)))


def test_source_rm_translations(owm_project):
    owm = owm_project.owm()
    with owm.connect() as conn:
        DS = owm.default_context(DataSource)
        ds0 = DS(key='ds0')
        tl1 = owm.default_context(Translation)(key='tl1')
        ds0.translation(tl1)
        owm.default_context.add_import(DataSource.definition_context)
        conn.mapper.process_class(DataSource)
        with transaction.manager:
            owm.default_context.save()
            owm.default_context.save_imports(transitive=False)
            conn(DataSource.definition_context).save()
            conn.mapper.save()
        print(conn.rdf.serialize(format='n3'))
    owm_project.sh(f'owm source rm {ds0.identifier}')
    with owm_project.owm().connect(read_only=True) as conn:
        assert [] == list(conn.rdf.triples((tl1.identifier, None, None)))


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


def test_type_rm_no_resolve(owm_project):
    from .test_modules.owmclitest06_datasource import TestDataSource
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')
    print(owm_project.sh('owm save tests.test_modules.owmclitest06_datasource'))
    print(owm_project.sh(f'owm type rm {TestDataSource.rdf_type}'))
    owm = owm_project.owm()
    with owm.connect() as conn:
        assert conn.mapper.resolve_class(
                TestDataSource.rdf_type,
                TestDataSource.definition_context) is None


def test_save_class_resolve_class(owm_project):
    from .test_modules.owmclitest06_datasource import TestDataSource
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')
    print(owm_project.sh('owm save tests.test_modules.owmclitest06_datasource'))
    owm = owm_project.owm()
    with owm.connect() as conn:
        assert conn.mapper.resolve_class(
                TestDataSource.rdf_type,
                TestDataSource.definition_context) is not None


def test_contexts_list_imports(owm_project):
    owm = owm_project.owm()
    ctx1_id = 'http://example.org/context#ctx1'
    ctx2_id = 'http://example.org/context#ctx2'
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx1 = conn(Context)(ident=ctx1_id)
            ctx2 = conn(Context)(ident=ctx2_id)
            ctx1.add_import(ctx2)
            ctx1.save_imports()

    assert owm_project.sh(f'owm contexts list-imports {ctx1_id}') == f'{ctx2_id}\n'


def test_contexts_rm_import_not_listed(owm_project):
    owm = owm_project.owm()
    ctx1_id = 'http://example.org/context#ctx1'
    ctx2_id = 'http://example.org/context#ctx2'
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx1 = conn(Context)(ident=ctx1_id)
            ctx2 = conn(Context)(ident=ctx2_id)
            ctx1.add_import(ctx2)
            ctx1.save_imports()

    owm_project.sh(f'owm contexts rm-import {ctx1_id} {ctx2_id}')
    assert owm_project.sh(f'owm contexts list-imports {ctx1_id}') == ''


def test_contexts_add_import(owm_project):
    owm = owm_project.owm()
    ctx1_id = 'http://example.org/context#ctx1'
    ctx2_id = 'http://example.org/context#ctx2'

    owm_project.sh(f'owm contexts add-import {ctx1_id} {ctx2_id}')
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx1 = conn(Context)(ident=ctx1_id)
            assert URIRef(ctx2_id) in [x.identifier for x in ctx1.stored.imports]


def test_rm_context_removes_all_1(owm_project):
    owm = owm_project.owm()
    pred = URIRef('http://example.org/p')
    with owm.connect() as conn:
        g = conn.rdf.graph(URIRef('http://example.org/ctx1'))
        for i in range(5):
            g.add((URIRef(f'http://example.org/s{i}'),
                pred,
                URIRef(f'http://example.org/o{i}'),))
    owm_project.sh('owm contexts rm http://example.org/ctx1')
    with owm.connect(read_only=True) as conn:
        assert set() == set(conn.rdf.triples((None, pred, None)))


def test_rm_context_removes_all_2(owm_project):
    owm = owm_project.owm()
    pred = URIRef('http://example.org/p')
    with owm.connect() as conn:
        g = conn.rdf.graph(URIRef('http://example.org/ctx1'))
        for i in range(5):
            g.add((URIRef(f'http://example.org/s{i}'),
                pred,
                URIRef(f'http://example.org/o{i}'),))
        g = conn.rdf.graph(URIRef('http://example.org/ctx2'))
        for i in range(5):
            g.add((URIRef(f'http://example.org/s{i}x'),
                pred,
                URIRef(f'http://example.org/o{i}x'),))
    with owm.connect(read_only=True) as conn:
        assert set() != set(conn.rdf.triples((None, pred, None)))
    owm_project.sh('owm contexts rm http://example.org/ctx1 http://example.org/ctx2')
    with owm.connect(read_only=True) as conn:
        assert set() == set(conn.rdf.triples((None, pred, None)))


def test_contexts_list(owm_project):
    owm = owm_project.owm()
    ctx1_id = 'http://example.org/context#ctx1'
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            conn.rdf.get_context(URIRef(ctx1_id)).add(
                    (EX.s, EX.p, EX.o))

    output = owm_project.sh('owm contexts list').strip()
    assert output.split('\n') == [ctx1_id]


def test_contexts_list_include_default(owm_project):
    owm = owm_project.owm()
    ctx1_id = 'http://example.org/context#ctx1'
    with owm.connect() as conn:
        with transaction.manager:
            # Create data sources
            conn.rdf.get_context(URIRef(ctx1_id)).add(
                    (EX.s, EX.p, EX.o))

    output = owm_project.sh('owm contexts list --include-default').strip()
    assert output.split('\n') == [ctx1_id, str(DATASET_DEFAULT_GRAPH_ID)]


def test_contexts_list_include_deps(tmp_path, owm_project):

    ctxa_id = 'http://example.org/context_a'
    ctxb_id = 'http://example.org/context_b'

    owm = owm_project.owm()
    with owm.connect() as conn, transaction.manager:
        conn.rdf.get_context(URIRef(ctxa_id)).add(
                (EX.s0, EX.p, EX.o))

    descr_a = Descriptor(
        'test/abundle',
        version=1,
        includes=[ctxa_id])
    apth = p(tmp_path, 'a.yml')
    write_descriptor(descr_a, apth)
    owm.bundle.install(apth)

    owm_project.owmdir = p(owm_project.testdir, '.owm1')

    owm1 = owm_project.owm(non_interactive=True)
    default_ctxid = 'http://example.org/project_default_context'
    owm1.init(default_context_id=default_ctxid)
    owm1.config.set('dependencies', json.dumps([{'id': descr_a.id, 'version': descr_a.version}]))

    with owm1.connect() as conn, transaction.manager:
        conn.rdf.get_context(URIRef(ctxb_id)).add(
                (EX.s1, EX.p, EX.o))

    base_cmd = f'owm --owmdir={owm_project.owmdir} contexts list'

    output = owm_project.sh(f'{base_cmd}').strip()
    assert set(output.split('\n')) == set([ctxb_id])

    output = owm_project.sh(f'{base_cmd} --include-dependencies').strip()
    assert set(output.split('\n')) == set([ctxa_id, ctxb_id])


def test_subclass_across_bundles(tmp_path, owm_project):

    ctxa_id = 'http://example.org/context_a'
    ctxb_id = 'http://example.org/context_b'
    ctxc_id = 'http://example.org/context_c'

    class A(DataObject):
        class_context = ctxa_id

    class B(A):
        class_context = ctxb_id

    class C(B):
        class_context = ctxc_id

    owm = owm_project.owm()
    with owm.connect() as conn:
        actx = conn(A.definition_context)
        conn.mapper.process_class(A)
        actx.save()

        bctx = conn(B.definition_context)
        conn.mapper.process_class(B)
        bctx.add_import(actx)
        bctx.save()
        bctx.save_imports()

        cctx = conn(C.definition_context)
        cctx.add_import(bctx)
        cctx.save()
        cctx.save_imports()

    descr_a = Descriptor(
        'test/abundle',
        version=1,
        includes=[ctxa_id])
    apth = p(tmp_path, 'a.yml')
    write_descriptor(descr_a, apth)

    descr_b = Descriptor(
        'test/bbundle',
        version=1,
        includes=[ctxb_id],
        dependencies=[(descr_a.id, descr_a.version)])
    bpth = p(tmp_path, 'b.yml')
    write_descriptor(descr_b, bpth)

    descr_c = Descriptor(
        'test/cbundle',
        version=1,
        includes=[ctxc_id],
        dependencies=[(descr_b.id, descr_b.version)])
    cpth = p(tmp_path, 'c.yml')
    write_descriptor(descr_c, cpth)

    owm.bundle.install(apth)
    owm.bundle.install(bpth)
    owm.bundle.install(cpth)

    owm_project.owmdir = p(owm_project.testdir, '.owm1')

    owm1 = owm_project.owm(non_interactive=True)
    default_ctxid = 'http://example.org/project_default_context'
    owm1.init(default_context_id=default_ctxid)
    owm1.config.set('dependencies', json.dumps([{'id': descr_c.id, 'version': descr_c.version}]))

    owm2 = owm_project.owm(non_interactive=True)
    with owm2.connect() as conn1:
        defctx = conn1(Context)(default_ctxid)
        defctx.add_import(cctx)
        c = defctx(C)(key="c")
        defctx.save()
        defctx.save_imports()

        loaded = [x.identifier for x in defctx.stored(A)().load()]
        assert loaded == [c.identifier]


def write_descriptor(descr, path):
    with open(path, 'w') as f:
        descr.dump(f)


def test_namespace_list(owm_project):
    with owm_project.owm().connect() as conn:
        conn.rdf.namespace_manager.bind('test_namespace', EX)

    namespaces = owm_project.sh('owm namespace list')
    assert 'prefix\ttest_namespace' in namespaces
    assert f'uri\t{EX}' in namespaces


def test_namespace_bind(owm_project):
    owm_project.sh(f'owm namespace bind test_namespace {EX}')
    with owm_project.owm().connect() as conn:
        assert ('test_namespace', URIRef(EX)) in set(conn.rdf.namespace_manager.namespaces())


def test_declare(owm_project):
    owm_project.sh(f'owm declare owmeta_core.dataobject:DataObject --id="{EX.bathtub}"')
    with owm_project.owm().connect() as conn:
        dctx = conn.owm.default_context
        objs = list(dctx.stored(DataObject)().load())
        assert objs[0].identifier == EX.bathtub


def test_declare_unknown_property_class(owm_project):
    cname = 'not.a.module:NotAClass'
    with raises(Exception, match=cname):
        owm_project.sh(f'owm declare owmeta_core.dataobject:DataObject {cname}=123 --id="{EX.duck}"')


def test_regendb(owm_project):
    owm = owm_project.owm()
    with owm.connect() as conn, transaction.manager:
        conn.rdf.add((EX.s, EX.p, EX.o))
    owm_project.sh('owm commit -m "commit 1"')
    with owm.connect() as conn, transaction.manager:
        conn.rdf.add((EX.s, EX.p, EX.o1))
    owm_project.sh('owm regendb')

    with owm.connect(read_only=True) as conn, transaction.manager:
        assert (EX.s, EX.p, EX.o) in conn.rdf
        assert (EX.s, EX.p, EX.o1) not in conn.rdf
