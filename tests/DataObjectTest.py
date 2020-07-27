from __future__ import absolute_import
from __future__ import print_function

from importlib import import_module
import re
import unittest
import warnings

import rdflib as R
import six

from owmeta_core import BASE_CONTEXT
from owmeta_core.graph_object import IdentifierMissingException
from owmeta_core.data import DataUser
from owmeta_core.dataobject import (DataObject,
                                    ObjectProperty,
                                    DatatypeProperty,
                                    UnionProperty,
                                    _partial_property,
                                    PythonModule,
                                    PythonClassDescription,
                                    RegistryEntry,
                                    Module)
from owmeta_core.context import Context
from owmeta_core.rdf_query_util import get_most_specific_rdf_type
from owmeta_core.utils import FCN

from .DataTestTemplate import _DataTest
from .TestUtilities import captured_logging

try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch


DATAOBJECT_PROPERTIES = ['DatatypeProperty', 'ObjectProperty', 'UnionProperty']


class DataObjectTest(_DataTest):
    ctx_classes = (DataObject,)

    def setUp(self):
        super(DataObjectTest, self).setUp()
        self.patcher = patch('owmeta_core.data', 'ALLOW_UNCONNECTED_DATA_USERS', True)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        super(DataObjectTest, self).tearDown()

    def test_DataUser(self):
        do = DataObject()
        self.assertTrue(isinstance(do, DataUser))

    def test_identifier(self):
        """ Test that we can set and return an identifier """
        do = DataObject(ident="http://example.org")
        self.assertEqual(do.identifier, R.URIRef("http://example.org"))

    def test_cls_object_from_id_type(self):
        '''
        Calling object_from_id on the class should search for the rdf_type
        '''
        ctx = Mock()
        DataObject.contextualize(ctx).object_from_id('http://openworm.org/some_rdf_type')
        ctx.resolve_class.assert_called()

    def test_repr(self):
        self.assertRegexpMatches(repr(DataObject(ident="http://example.com")),
                                 r"DataObject\(ident=rdflib\.term\.URIRef\("
                                 r"u?[\"']http://example.com[\"']\)\)")

    def test_properties_are_init_args(self):
        class A(DataObject):
            a = DatatypeProperty()
            properties_are_init_args = True
        a = A(a=5)
        self.assertEqual(5, a.a())

    def test_properties_are_init_args_subclass_override(self):
        class A(DataObject):
            a = DatatypeProperty()
            properties_are_init_args = True

        class B(A):
            b = DatatypeProperty()
            properties_are_init_args = False

        if six.PY2:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                B(a=5)
                self.assertTrue(len(w) > 0 and issubclass(w[0].category, DeprecationWarning))
        else:
            with self.assertRaises(TypeError):
                B(a=5)

    def test_properties_are_init_args_subclass_parent_unchanged(self):
        class A(DataObject):
            a = DatatypeProperty()
            properties_are_init_args = True

        class B(A):
            b = DatatypeProperty()
            properties_are_init_args = False

        a = A(a=5)
        self.assertEqual(5, a.a())

    def test_properties_are_init_args_subclass_explicit(self):
        class A(DataObject):
            a = DatatypeProperty()
            properties_are_init_args = True

        class B(A):
            def __init__(self, a=None, **kw):
                super(B, self).__init__(**kw)
                pass

        b = B(a=5)
        self.assertIsNone(b.a())

    def test_rdfs_comment_property(self):
        a = DataObject(rdfs_comment='Hello')
        self.assertIn('Hello', a.rdfs_comment())

    def test_context_getter(self):
        a = DataObject()
        self.assertIsNone(a.context)

    def test_context_setter(self):
        a = DataObject()
        a.context = 42
        self.assertEquals(a.context, 42)

    def test_dataobject_property_that_generate_partial_property(self):
        for property_classmethod in DATAOBJECT_PROPERTIES:
            partial_property = getattr(DataObject, property_classmethod)()
            self.assertIsInstance(partial_property, _partial_property)

    def test_dataobject_property_that_return_owner(self):
        for property_classmethod in DATAOBJECT_PROPERTIES:
            owner = Mock()
            getattr(DataObject, property_classmethod)(owner=owner, linkName="")
            owner.attach_property.assert_called_once()

    def test_query_identifier(self):
        class A(DataObject):
            @property
            def identifier(self):
                return R.URIRef('http://example.org/idid')

        self.assertEquals(A.query().identifier, R.URIRef('http://example.org/idid'))

    def test_query_identifier_augment(self):
        class A(DataObject):
            def identifier_augment(self):
                return R.URIRef('http://example.org/idid')

        with self.assertRaises(IdentifierMissingException):
            A.query().identifier

    def test_query_defined(self):
        class A(DataObject):
            @property
            def defined(self):
                return True

        self.assertTrue(A.query().defined)

    def test_query_defined_augment(self):
        class A(DataObject):
            def defined_augment(self):
                return True

        self.assertFalse(A.query().defined)

    def test_query_cname(self):
        class A(DataObject):
            pass

        self.assertEquals(A.__name__, A.query.__name__)

    def test_query_module(self):
        class A(DataObject):
            pass

        self.assertEquals(A.__module__, A.query.__module__)

    def test_query_rdf_type(self):
        class A(DataObject):
            pass

        self.assertEquals(A.rdf_type, A.query.rdf_type)

    def test_query_py_type(self):
        class A(DataObject):
            pass

        self.assertIs(type(A), type(A.query))

    def test_query_context(self):
        class A(DataObject):
            pass

        ctx = Context(ident='http://example.org/texas')
        ctxd = ctx(A)
        qctxd = ctxd.query
        self.assertIs(ctxd.context, qctxd.context)

    def test_adhoc_datatype_property_in_context(self):
        class A(DataObject):
            a = DatatypeProperty()

        class B(DataObject):
            pass

        ctx = Context(ident='http://example.org/geogia')
        b = ctx(B)(key='b')
        A.a(b)('square')
        self.assertIn((R.URIRef('http://schema.openworm.org/2020/07/B#b'),
                       R.URIRef('http://schema.openworm.org/2020/07/A/a'),
                       R.Literal('square')), list(ctx.contents_triples()))

    def test_adhoc_object_property_in_context(self):
        class A(DataObject):
            a = ObjectProperty()

        class B(DataObject):
            pass

        ctx = Context(ident='http://example.org/alabama')
        b = ctx(B)(key='b')
        A.a(b)(A(key='a'))
        self.assertIn((R.URIRef('http://schema.openworm.org/2020/07/B#b'),
                       R.URIRef('http://schema.openworm.org/2020/07/A/a'),
                       R.URIRef('http://schema.openworm.org/2020/07/A#a')), list(ctx.contents_triples()))

    def test_adhoc_union_property_in_context(self):
        class A(DataObject):
            a = UnionProperty()

        class B(DataObject):
            pass

        ctx = Context(ident='http://example.org/wisconsin')
        b = ctx(B)(key='b')
        A.a(b)(A(key='a'))
        self.assertIn((R.URIRef('http://schema.openworm.org/2020/07/B#b'),
                       R.URIRef('http://schema.openworm.org/2020/07/A/a'),
                       R.URIRef('http://schema.openworm.org/2020/07/A#a')), list(ctx.contents_triples()))

    def test_adhoc_property_twice_same_instance(self):
        '''
        Technically, should still work if distinct Property instances are bound, esp.
        since they aren't bound to a name on the DataObject, but we prefer to use the same
        instance so some of the semantics as for pre-declared properties are the carried
        through.
        '''
        class A(DataObject):
            a = DatatypeProperty()

        class B(DataObject):
            pass

        ctx = Context(ident='http://example.org/maine')
        b = ctx(B)(key='b')
        self.assertIs(A.a(b), A.a(b))


class ClassRegistryTest(_DataTest):

    def setUp(self):
        super(ClassRegistryTest, self).setUp()
        self.mapper.process_classes(DataObject, PythonClassDescription, Module, PythonModule, RegistryEntry)
        mod = import_module('tests.DataObjectTest')
        try:
            delattr(mod, '__yarom_mapped_classes__')
        except AttributeError:
            pass

    def test_load_unloaded_subtype(self):
        '''
        This test actually combines a few different features:
            - loading a module from a ClassDescription
            - resolving subclasses from superclasses
        '''
        ident = R.URIRef('http://openworm.org/entities/TDO01')
        rdftype = R.RDF['type']
        sc = R.RDFS['subClassOf']
        tdo = R.URIRef('http://openworm.org/entities/TDO')
        pm = R.URIRef('http://example.com/pymod')
        pcd = R.URIRef('http://example.com/pycd')
        re = R.URIRef('http://example.com/re')
        g = R.ConjunctiveGraph()
        crctx = g.get_context(self.mapper.class_registry_context.identifier)
        ctx = g.get_context(self.context.identifier)
        self.TestConfig['rdf.graph'] = g
        trips = [(ident, rdftype, tdo),
                 (tdo, sc, DataObject.rdf_type)]
        for tr in trips:
            ctx.add(tr)
        trips = [(tdo, sc, DataObject.rdf_type),
                 (pm, rdftype, PythonModule.rdf_type),
                 (pm, PythonModule.name.link, R.Literal('tests.tmod.tdo')),
                 (pcd, PythonClassDescription.name.link, R.Literal('TDO')),
                 (pcd, rdftype, PythonClassDescription.rdf_type),
                 (pcd, PythonClassDescription.module.link, pm),
                 (re, rdftype, RegistryEntry.rdf_type),
                 (re, RegistryEntry.rdf_class.link, tdo),
                 (re, RegistryEntry.class_description.link, pcd)]
        for tr in trips:
            crctx.add(tr)
        o = list(self.context.stored(DataObject)(ident=ident).load())
        self.assertEqual('tests.tmod.tdo.TDO', FCN(type(o[0])))

    def test_save_load_subtype(self):

        class A(DataObject):
            class_context = self.context
        self.mapper.process_class(A)

        self.context.add_import(BASE_CONTEXT)
        m = self.context(Context)(ident='http://example.org/ctx', imported=(self.context,))
        im = self.context(Context)(ident='http://example.org/ctxim', imported=(self.context,))
        co = self.context(Context)(ident='http://example.org/ctxb', imported=(m, im))
        m(A)(ident='http://example.org/anA')
        co.save_imports(im)
        co.save_context(inline_imports=True)

        o = list(m.stored(DataObject)(ident='http://example.org/anA').load())
        self.assertIsInstance(o[0], A)

    def test_warning_for_class_not_in_module_dict(self):
        class A(DataObject):
            unmapped = True
            class_context = self.context

        with captured_logging() as logs:
            self.mapper.declare_python_class_registry_entry(A)
            log = logs.getvalue()
            self.assertRegexpMatches(log, 'registry')
            self.assertRegexpMatches(log, 'tests.DataObjectTest')
            self.assertRegexpMatches(log, r'\bA\b')

    def test_registry_in_yarom_mapped_class(self):
        class A(DataObject):
            class_context = self.context

        with captured_logging() as logs:
            mod = import_module('tests.DataObjectTest')
            mod.__yarom_mapped_classes__ = (A,)
            try:
                self.mapper.process_class(A)
                log = logs.getvalue()
                self.assertNotRegexpMatches(log, 'registry')
            finally:
                delattr(mod, '__yarom_mapped_classes__')

    def test_resolve_class_from_registry_in_ymc(self):
        class A(DataObject):
            class_context = self.context
            rdf_type = R.URIRef('http://example.org/A')

        # given
        mod = import_module('tests.DataObjectTest')
        self.mapper.process_class(A)
        self.mapper.declare_python_class_registry_entry(A)
        self.mapper.class_registry_context.save()

        mod.__yarom_mapped_classes__ = (A,)

        # when
        try:
            del self.mapper.RDFTypeTable[A.rdf_type]
            res = self.mapper.resolve_class(A.rdf_type)
            # then
            self.assertIsNotNone(res)
        finally:
            delattr(mod, '__yarom_mapped_classes__')

    def test_resolve_class_not_in_ymc(self):
        class A(DataObject):
            class_context = self.context
            rdf_type = R.URIRef('http://example.org/A')

        # given
        self.mapper.process_class(A)

        import_module('tests.DataObjectTest')

        # when
        del self.mapper.RDFTypeTable[A.rdf_type]

        # then
        self.assertIsNone(self.context.resolve_class(A.rdf_type))

    def test_resolve_class_multiple_entries_in_ymc(self):
        class A(DataObject):
            unmapped = True
            class_context = self.context
            rdf_type = R.URIRef('http://example.org/A0')

        # given
        mod = import_module('tests.DataObjectTest')
        self.mapper.process_class(A)
        self.mapper.declare_python_class_registry_entry(A)
        self.mapper.class_registry_context.save()

        mod.__yarom_mapped_classes__ = (A, A)

        with captured_logging() as logs:
            # when
            try:
                del self.mapper.RDFTypeTable[A.rdf_type]
                self.mapper.resolve_class(A.rdf_type)
                # then
                self.assertRegexpMatches(logs.getvalue(), r'More than one.*__yarom_mapped_classes__')
            finally:
                delattr(mod, '__yarom_mapped_classes__')


class ClassRegistryMissingModuleTest(_DataTest):
    def setUp(self):
        super(ClassRegistryMissingModuleTest, self).setUp()
        self.mapper.process_classes(DataObject, PythonClassDescription, Module, PythonModule, RegistryEntry)
        mod = import_module('tests.DataObjectTest')
        try:
            delattr(mod, '__yarom_mapped_classes__')
        except AttributeError:
            pass
        ident = R.URIRef('http://openworm.org/entities/TDO01')
        rdftype = R.RDF['type']
        sc = R.RDFS['subClassOf']
        tdo = R.URIRef('http://openworm.org/entities/TDO')
        pm = R.URIRef('http://example.com/pymod')
        pcd = R.URIRef('http://example.com/pycd')
        re = R.URIRef('http://example.com/re')
        g = R.ConjunctiveGraph()
        crctx = g.get_context(self.mapper.class_registry_context.identifier)
        ctx = g.get_context(self.context.identifier)
        self.TestConfig['rdf.graph'] = g
        trips = [(ident, rdftype, tdo),
                 (tdo, sc, DataObject.rdf_type)]
        for tr in trips:
            ctx.add(tr)
        trips = [(tdo, sc, DataObject.rdf_type),
                 (pm, rdftype, PythonModule.rdf_type),
                 (pm, PythonModule.name.link, R.Literal('this.module.does.not.exist')),
                 (pcd, PythonClassDescription.name.link, R.Literal('TDO')),
                 (pcd, rdftype, PythonClassDescription.rdf_type),
                 (pcd, PythonClassDescription.module.link, pm),
                 (re, rdftype, RegistryEntry.rdf_type),
                 (re, RegistryEntry.rdf_class.link, tdo),
                 (re, RegistryEntry.class_description.link, pcd)]
        for tr in trips:
            crctx.add(tr)
        self.ident = ident

    def test_resolves_dataobject(self):
        o = list(self.context.stored(DataObject)(ident=self.ident).load())
        self.assertEqual(FCN(DataObject), FCN(type(o[0])))

    def test_warns(self):
        with captured_logging() as logs:
            list(self.context.stored(DataObject)(ident=self.ident).load())
            self.assertRegexpMatches(logs.getvalue(), r'Did not find module this.module.does.not.exist')


sc = R.RDFS['subClassOf']
rdftype = R.RDF['type']
tdo = R.URIRef('http://openworm.org/entities/TDO')
pm = R.URIRef('http://example.com/pymod')
pcd = R.URIRef('http://example.com/pycd')
re0 = R.URIRef('http://example.com/re0')


class ClassRegistryMissingClassTest(_DataTest):
    def setUp(self):
        super(ClassRegistryMissingClassTest, self).setUp()
        self.mapper.process_classes(DataObject, PythonClassDescription, Module, PythonModule, RegistryEntry)
        mod = import_module('tests.DataObjectTest')
        try:
            delattr(mod, '__yarom_mapped_classes__')
        except AttributeError:
            pass
        ident = R.URIRef('http://openworm.org/entities/TDO01')
        g = R.ConjunctiveGraph()
        self.crctx = g.get_context(self.mapper.class_registry_context.identifier)
        ctx = g.get_context(self.context.identifier)
        self.TestConfig['rdf.graph'] = g
        trips = [(ident, rdftype, tdo),
                 (tdo, sc, DataObject.rdf_type)]
        for tr in trips:
            ctx.add(tr)
        trips = [(tdo, sc, DataObject.rdf_type),
                 (pm, rdftype, PythonModule.rdf_type),
                 (pm, PythonModule.name.link, R.Literal('tests.tmod.tdo')),
                 (pcd, PythonClassDescription.name.link, R.Literal('NotTDO')),
                 (pcd, rdftype, PythonClassDescription.rdf_type),
                 (pcd, PythonClassDescription.module.link, pm),
                 (re0, rdftype, RegistryEntry.rdf_type),
                 (re0, RegistryEntry.rdf_class.link, tdo),
                 (re0, RegistryEntry.class_description.link, pcd)]
        for tr in trips:
            self.crctx.add(tr)
        self.ident = ident

    def test_resolves_dataobject(self):
        o = list(self.context.stored(DataObject)(ident=self.ident).load())
        self.assertEqual(FCN(DataObject), FCN(type(o[0])))

    def test_warns(self):
        with captured_logging() as logs:
            list(self.context.stored(DataObject)(ident=self.ident).load())
            self.assertRegexpMatches(logs.getvalue(),
                    re.compile(r'Did not find class NotTDO in tests.tmod.tdo$',
                        flags=re.MULTILINE))

    def test_warns_ymc(self):
        with captured_logging() as logs:
            list(self.context.stored(DataObject)(ident=self.ident).load())
            self.assertRegexpMatches(logs.getvalue(),
                    r'Did not find class NotTDO in tests.tmod.tdo.__yarom_mapped_classes__')

    def test_failover(self):
        pcd0 = R.URIRef('http://example.com/pycd0')
        re1 = R.URIRef('http://example.com/re1')
        trips = [(pcd0, PythonClassDescription.name.link, R.Literal('TDO')),
                 (pcd0, rdftype, PythonClassDescription.rdf_type),
                 (pcd0, PythonClassDescription.module.link, pm),
                 (re1, rdftype, RegistryEntry.rdf_type),
                 (re1, RegistryEntry.rdf_class.link, tdo),
                 (re1, RegistryEntry.class_description.link, pcd0)]
        for tr in trips:
            self.crctx.add(tr)
        o = list(self.context.stored(DataObject)(ident=self.ident).load())
        self.assertEqual('tests.tmod.tdo.TDO', FCN(type(o[0])))


class KeyPropertiesTest(_DataTest):

    def test_defined(self):
        class A(DataObject):
            a = DatatypeProperty()
            b = DatatypeProperty()
            key_properties = ('a', 'b')

        a = A()
        a.a('hello')
        a.b('dolly')
        self.assertTrue(a.defined)

    def test_undef(self):
        class A(DataObject):
            a = DatatypeProperty()
            b = DatatypeProperty()
            key_properties = ('a', 'b')

        a = A()
        a.a('hello')
        self.assertFalse(a.defined)

    def test_ident(self):
        class A(DataObject):
            a = DatatypeProperty()
            b = DatatypeProperty()
            key_properties = ('a', 'b')

        a = A()
        a.a('hello')
        a.b('dolly')
        self.assertIsInstance(a.identifier, R.URIRef)

    def test_ident_pass_ident(self):
        class A(DataObject):
            a = DatatypeProperty()
            b = DatatypeProperty()
            key_properties = (a, b)

        a = A()
        a.a('hello')
        a.b('dolly')
        self.assertIsInstance(a.identifier, R.URIRef)

    def test_ident_pass_ident_and_string(self):
        class A(DataObject):
            a = DatatypeProperty()
            b = DatatypeProperty()
            key_properties = (a, 'b')

        a = A()
        a.a('hello')
        a.b('dolly')
        self.assertIsInstance(a.identifier, R.URIRef)

    def test_ident_pass_ident_and_string_from_parent(self):
        class B(DataObject):
            b = DatatypeProperty()

        class A(B):
            a = DatatypeProperty()
            key_properties = (a, 'b')

        a = A()
        a.a('hello')
        a.b('dolly')
        self.assertIsInstance(a.identifier, R.URIRef)

    def test_ident_undef_ident_and_string_from_parent(self):
        class B(DataObject):
            prop = DatatypeProperty()

        class A(B):
            a = DatatypeProperty()
            key_properties = (a, 'prop')

        a = A()
        a.a('hello')
        self.assertFalse(a.defined)

    def test_ident_undef_ident_and_ident_from_parent(self):
        class B(DataObject):
            prop = DatatypeProperty()

        class A(B):
            a = DatatypeProperty()
            key_properties = (a, B.prop)

        a = A()
        a.a('hello')
        self.assertFalse(a.defined)

    def test_error_non_property_pthunk(self):
        with self.assertRaisesRegexp(Exception, r'\bcookie\b'):
            class B(DataObject):
                a = DatatypeProperty()
                key_properties = (a, DatatypeProperty(name="cookie"))

    def test_error_non_property_PropertyProperty(self):
        class A(DataObject):
            cookie = DatatypeProperty()

        with self.assertRaisesRegexp(Exception, r'cookie'):
            class B(DataObject):
                prop = DatatypeProperty()
                key_properties = (prop, A.cookie)

    def test_object_property_ident(self):
        class A(DataObject):
            a = ObjectProperty()
            b = DatatypeProperty()
            key_properties = ('a', 'b')

        o1 = A()
        o2 = A(ident='http://example.org/o2')
        o1.a(o2)
        o1.b('dolly')
        self.assertIsInstance(o1.identifier, R.URIRef)

    def test_missing_properties(self):
        class A(DataObject):
            a = ObjectProperty()
            key_properties = ('a', 'not_an_attr')

        o1 = A()
        o2 = A(ident='http://example.org/o2')
        o1.a(o2)
        with self.assertRaisesRegexp(Exception, r'\bnot_an_attr\b'):
            o1.defined


class GMSRTTest(unittest.TestCase):
    '''
    Just covering some edge cases here...other unit tests cover query utils pretty well
    '''

    def test_no_context_no_bases_return_types0(self):
        t1 = R.URIRef('http://example.org/t1')
        self.assertEqual(t1, get_most_specific_rdf_type(types={t1}))

    def test_no_context_return_types0(self):
        t1 = R.URIRef('http://example.org/t1')
        self.assertEqual(t1, get_most_specific_rdf_type(types={t1}, base=t1))

    def test_no_context_no_types_return_bases0(self):
        t1 = R.URIRef('http://example.org/t1')
        self.assertEqual(t1, get_most_specific_rdf_type(types=set(), base=t1))

    def test_no_context_no_types_no_bases_no_mst(self):
        self.assertIsNone(get_most_specific_rdf_type(types=set()))

    def test_no_context_many_types_no_bases_no_mst(self):
        t1 = R.URIRef('http://example.org/t1')
        t2 = R.URIRef('http://example.org/t2')
        self.assertIsNone(get_most_specific_rdf_type(types={t1, t2}))

    def test_no_context_one_type_different_bases_no_mst(self):
        t1 = R.URIRef('http://example.org/t1')
        t2 = R.URIRef('http://example.org/t2')
        self.assertIsNone(get_most_specific_rdf_type(types={t1}, base=t2))

    def test_context_with_no_mapper_or_bases(self):
        ctx = Mock()
        ctx.mapper = None
        self.assertIsNone(get_most_specific_rdf_type(types=set(), context=ctx))

    def test_context_with_no_mapper_and_bases_context_doesnt_know(self):
        ctx = Mock()
        ctx.resolve_class.return_value = None
        ctx.mapper = None
        self.assertIsNone(get_most_specific_rdf_type(types=set(), context=ctx))

    def test_context_with_mapper_and_bases_context_doesnt_know(self):
        ctx = Mock()
        ctx.resolve_class.return_value = None
        t1 = Mock()
        t2 = Mock()
        t1.rdf_type = R.URIRef('http://example.org/t1')
        t2.rdf_type = R.URIRef('http://example.org/t2')
        ctx.mapper.base_classes.values.return_value = {t1, t2}
        self.assertIsNone(get_most_specific_rdf_type(types=set(), context=ctx))
