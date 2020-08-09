from __future__ import print_function
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

from .test_modules.owmclitest01 import DT2
from .TestUtilities import assertRegexpMatches

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
    owm = OWM(owmdir=p(owm_project.testdir, '.owm'))
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
