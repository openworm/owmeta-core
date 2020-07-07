from __future__ import print_function
from __future__ import absolute_import
import rdflib as R

from .DataTestTemplate import _DataTest

from owmeta_core.statement import Statement
from owmeta_core.property_value import PropertyValue
from owmeta_core.dataobject import DataObject
from owmeta_core.context import Context


class DataobjectPropertyTest(_DataTest):
    ctx_classes = (DataObject,)

    def setUp(self):
        super(DataobjectPropertyTest, self).setUp()
        from owmeta_core.dataobject import PropertyTypes
        PropertyTypes.clear()

    def tearDown(self):
        super(DataobjectPropertyTest, self).tearDown()
        from owmeta_core.dataobject import PropertyTypes
        PropertyTypes.clear()

    # XXX: auto generate some of these tests...
    def test_same_value_same_id_empty(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do1 = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        c = DataObject.DatatypeProperty("boots", do)
        c1 = DataObject.DatatypeProperty("boots", do1)
        self.assertEqual(c.identifier, c1.identifier)

    def test_same_value_same_id_not_empty(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do1 = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        c = DataObject.DatatypeProperty("boots", do)
        c1 = DataObject.DatatypeProperty("boots", do1)
        do.boots('partition')
        do1.boots('partition')
        self.assertEqual(c.identifier, c1.identifier)

    def test_same_value_same_id_not_empty_object_property(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do1 = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        dz = self.ctx.DataObject(ident=R.URIRef("http://example.org/vip"))
        dz1 = self.ctx.DataObject(ident=R.URIRef("http://example.org/vip"))
        c = DataObject.ObjectProperty("boots", do)
        c1 = DataObject.ObjectProperty("boots", do1)
        do.boots(dz)
        do1.boots(dz1)
        self.assertEqual(c.identifier, c1.identifier)

    def test_diff_value_diff_id_equal(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do1 = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        c = DataObject.DatatypeProperty("boots", do)
        c1 = DataObject.DatatypeProperty("boots", do1)
        do.boots('join')
        do1.boots('partition')
        self.assertEqual(c.identifier, c1.identifier)

    def test_diff_prop_same_name_same_object_same_value_same_id(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        c = DataObject.DatatypeProperty("boots", do)
        c1 = DataObject.DatatypeProperty("boots", do)
        c('join')
        c1('join')
        self.assertEqual(c.identifier, c1.identifier)

    def test_diff_prop_same_name_same_object_diff_value_same_id(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        c = DataObject.DatatypeProperty("boots", do)
        c1 = DataObject.DatatypeProperty("boots", do)
        c('partition')
        c1('join')
        self.assertEqual(c.identifier, c1.identifier)

    def test_diff_value_insert_order_same_id(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do1 = self.ctx.DataObject(ident=R.URIRef("http://example.org"))

        print(list(self.context.contents_triples()))
        c = DataObject.DatatypeProperty("boots", do, multiple=True)
        c1 = DataObject.DatatypeProperty("boots", do1, multiple=True)
        do.boots('join')
        do.boots('simile')
        do.boots('partition')
        do1.boots('partition')
        do1.boots('join')
        do1.boots('simile')
        self.assertEqual(c.identifier, c1.identifier)

    def test_object_property_diff_value_insert_order_same_id(self):
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do1 = self.ctx.DataObject(ident=R.URIRef("http://example.org"))

        oa = self.ctx.DataObject(ident=R.URIRef("http://example.org/a"))
        ob = self.ctx.DataObject(ident=R.URIRef("http://example.org/b"))
        oc = self.ctx.DataObject(ident=R.URIRef("http://example.org/c"))

        c = DataObject.ObjectProperty("boots", do, multiple=True)
        c1 = DataObject.ObjectProperty("boots", do1, multiple=True)

        do.boots(oa)
        do.boots(ob)
        do.boots(oc)

        do1.boots(oc)
        do1.boots(oa)
        do1.boots(ob)

        self.assertEqual(c.identifier, c1.identifier)

    def test_property_get_returns_collection(self):
        """
        This is for issue #175.
        """

        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do.boots = DataObject.DatatypeProperty(multiple=True)
        do.boots(4)
        # self.save()

        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        do.boots = DataObject.DatatypeProperty(multiple=True)

        x = do.boots()
        l1 = list(x)
        print(l1)
        b = list(x)
        self.assertEqual([4], b)

    def test_defined_statements_across_contexts_datatype_property(self):
        '''
        Statements have the Context included as a regular attribute, so we don't filter
        by the property's current context
        '''
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        ctx = Context('http://example.org/ctx/')
        do.birds = DataObject.DatatypeProperty(multiple=True)
        ctx(do).birds(4)
        do.birds(5)
        stmts = list(ctx(do).birds.defined_statements)
        assert stmts == [Statement(do, do.birds, PropertyValue(R.Literal(4)), ctx),
                         Statement(do, do.birds, PropertyValue(R.Literal(5)), self.context)]

    def test_defined_statements_across_contexts_object_property(self):
        '''
        Statements have the Context included as a regular attribute, so we don't filter
        by the property's current context
        '''
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org/1"))
        ctx = Context('http://example.org/ctx/')
        do.bugs = DataObject.ObjectProperty(multiple=True)
        dp = self.ctx.DataObject(ident=R.URIRef("http://example.org/2"))
        ctx(do).bugs(do)
        do.bugs(dp)
        stmts = list(ctx(do).bugs.defined_statements)
        assert stmts == [Statement(do, do.bugs, do, ctx),
                         Statement(do, do.bugs, dp, self.context)]

    def test_statements_staged(self):
        '''
        Statements have the Context included as a regular attribute, so we don't filter
        by the property's current context.

        The property's `rdf` attribute evals to the context's staged graph, so we get an
        "extra" entry from the context
        '''
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        ctx = Context('http://example.org/ctx/')
        do.birds = DataObject.DatatypeProperty(multiple=True)
        ctx(do).birds(4)
        do.birds(5)
        stmts = list(do.birds.statements)
        for s in stmts:
            print(s.to_quad())
        # Split up into 3 asserts so you can actually read pytest' error print-out...
        assert stmts[0] == Statement(do, do.birds, PropertyValue(R.Literal(4)), ctx)
        assert stmts[1] == Statement(do, do.birds, PropertyValue(R.Literal(5)), self.context)

        # These statements are not actually equal because statements mints a new Context
        # for what it retrieves from the RDF graph (it has to)
        assert stmts[2].to_quad() == Statement(do, do.birds, PropertyValue(R.Literal(5)), self.context).to_quad()

    def test_statements_query_empty(self):
        '''
        Statements have the Context included as a regular attribute, so we don't filter
        by the property's current context.

        The property's `rdf` attribute evals to the context's staged graph, so we get an
        "extra" entry from the context
        '''
        do = self.ctx.DataObject(ident=R.URIRef("http://example.org"))
        ctx = Context('http://example.org/ctx/')
        do.birds = DataObject.DatatypeProperty(multiple=True)
        ctx(do).birds(4)
        do.birds(5)
        stmts = list(Context('http://example.org/ctx/')(do).birds.statements)
        assert stmts == [Statement(do, do.birds, PropertyValue(R.Literal(4)), ctx),
                         Statement(do, do.birds, PropertyValue(R.Literal(5)), self.context)]
