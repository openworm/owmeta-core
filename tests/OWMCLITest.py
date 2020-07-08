from __future__ import print_function
from rdflib.term import URIRef
from os.path import join as p
import os
import re
import transaction
from pytest import mark

from owmeta_core.data_trans.local_file_ds import LocalFileDataSource as LFDS
from owmeta_core.datasource import DataTranslator
from owmeta_core.command import OWM
from owmeta_core.context import Context, IMPORTS_CONTEXT_KEY, DEFAULT_CONTEXT_KEY
from owmeta_core.context_common import CONTEXT_IMPORTS

from .TestUtilities import assertRegexpMatches

pytestmark = mark.owm_cli_test


def test_save_diff(owm_project):
    ''' Change something and make a diff '''
    modpath = p(owm_project.testdir, 'test_module')
    os.mkdir(modpath)
    open(p(modpath, '__init__.py'), 'w').close()
    owm_project.writefile(p(modpath, 'command_test_save.py'), '''\
        from test_module.monkey import Monkey


        def owm_data(ns):
            ns.context.add_import(Monkey.definition_context)
            ns.context(Monkey)(bananas=55)
        ''')

    owm_project.writefile(p(modpath, 'monkey.py'), '''\
        from owmeta_core.dataobject import DataObject, DatatypeProperty


        class Monkey(DataObject):
            class_context = 'http://example.org/primate/monkey'

            bananas = DatatypeProperty()
            def identifier_augment(owm_project):
                return type(owm_project).rdf_namespace['paul']

            def defined_augment(owm_project):
                return True


        __yarom_mapped_classes__ = (Monkey,)
        ''')
    print(owm_project.sh('owm save test_module.command_test_save'))
    assertRegexpMatches(owm_project.sh('owm diff'), r'<[^>]+>')


def test_save_classes(owm_project):
    modpath = p(owm_project.testdir, 'test_module')
    os.mkdir(modpath)
    open(p(modpath, '__init__.py'), 'w').close()
    owm_project.writefile(p(modpath, 'monkey.py'), '''\
        from owmeta_core.dataobject import DataObject, DatatypeProperty


        class Monkey(DataObject):
            class_context = 'http://example.org/primate/monkey'

            bananas = DatatypeProperty()
            def identifier_augment(owm_project):
                return type(owm_project).rdf_namespace['paul']

            def defined_augment(owm_project):
                return True


        __yarom_mapped_classes__ = (Monkey,)
        ''')
    print(owm_project.sh('owm save test_module.monkey'))
    assertRegexpMatches(owm_project.sh('owm diff'), r'<[^>]+>')


def test_save_imports(owm_project):
    modpath = p(owm_project.testdir, 'test_module')
    os.mkdir(modpath)
    open(p(modpath, '__init__.py'), 'w').close()
    owm_project.writefile(p(modpath, 'monkey.py'), '''\
        from owmeta_core.dataobject import DataObject, DatatypeProperty

        class Monkey(DataObject):
            class_context = 'http://example.org/primate/monkey'

            bananas = DatatypeProperty()
            def identifier_augment(self):
                return type(self).rdf_namespace['paul']

            def defined_augment(self):
                return True


        class Giraffe(DataObject):
            class_context = 'http://example.org/ungulate/giraffe'


        def owm_data(ns):
            ns.context.add_import(Monkey.definition_context)
            ns.context.add_import(Giraffe.definition_context)

        __yarom_mapped_classes__ = (Monkey,)
        ''')
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

    def translate(source):
        pass


def test_translator_list(owm_project):
    expected = URIRef('http://example.org/trans1')
    with OWM(owmdir=p(owm_project.testdir, '.owm')).connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = Context(ident='http://example.org/context', conf=conn.conf)
            ctx.mapper.process_class(DT1)

            DT1.definition_context.save(conn.conf['rdf.graph'])
            # Create a translator
            ctx(DT1)()

            ctx_id = conn.conf[DEFAULT_CONTEXT_KEY]
            main_ctx = Context(ident=ctx_id, conf=conn.conf)
            main_ctx.add_import(ctx)
            main_ctx.save_imports()
            ctx.save()

    # List translators
    assertRegexpMatches(
        owm_project.sh('owm -o table translator list'),
        re.compile(expected.n3(), flags=re.MULTILINE)
    )


class DT2(DataTranslator):
    class_context = 'http://example.org/context'
    input_type = LFDS
    output_type = LFDS
    translator_identifier = 'http://example.org/trans1'

    def translate(source):
        print(source.full_path())
        return source


def test_translate_data_source_loader(owm_project):
    with OWM(owmdir=p(owm_project.testdir, '.owm')).connect() as conn:
        with transaction.manager:
            # Create data sources
            ctx = Context(ident='http://example.org/context', conf=conn.conf)
            ctx(LFDS)(
                ident='http://example.org/lfds',
                file_name='Merged_Nuclei_Stained_Worm.zip',
                torrent_file_name='d9da5ce947c6f1c127dfcdc2ede63320.torrent'
            )
            ctx.mapper.process_class(DT2)
            ctx(DT2)()
            # Create a translator
            ctx_id = conn.conf[DEFAULT_CONTEXT_KEY]
            DT2.definition_context.save(conn.conf['rdf.graph'])
            main_ctx = Context(ident=ctx_id, conf=conn.conf)
            main_ctx.add_import(ctx)
            main_ctx.save_imports()
            ctx.save()

    # Do translation
    assertRegexpMatches(
        owm_project.sh('owm translate http://example.org/trans1 http://example.org/lfds'),
        r'Merged_Nuclei_Stained_Worm.zip'
    )
