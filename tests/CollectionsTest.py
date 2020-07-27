from __future__ import absolute_import

from itertools import islice

from rdflib.term import URIRef

from owmeta_core.collections import Bag, List

from .DataTestTemplate import _DataTest
from .TestUtilities import captured_logging


class BagTest(_DataTest):

    def test_bag_init(self):
        b = Bag(name="bah", value=12)
        b.value(55)
        b.add(545)
        b.value(Bag(name="humbug"))
        self.assertEqual(Bag.rdf_namespace['bah'], b.identifier)


class ListTest(_DataTest):
    ctx_classes = (List,)

    def test_init_singleton(self):
        cut = List(first="Danny Davenport",
                   rest=List.nil)
        self.assertEqual('Danny Davenport', cut.first())
        self.assertEqual(List.nil, cut.rest.onedef())

    def test_init(self):
        cut = List(ident="http://example.org/conway_academic_lineage",
                first="John Horton Conway",
                rest=List(ident="http://example.org/conway_academic_lineage#1",
                    first="Harold Davenport",
                    rest=List(ident="http://example.org/conway_academic_lineage#2",
                        first="John Edensor Littlewood",
                        rest=List(ident="http://example.org/conway_academic_lineage#3",
                            first="Ernest Barnes",
                            rest=List(ident="http://example.org/conway_academic_lineage#4",
                                first="W. W. Rouse Ball",
                                rest=List.nil)))))
        self.assertEqual('W. W. Rouse Ball',
                         cut.rest().rest().rest().rest().first())

    def test_make(self):
        cut = List.from_sequence(["John Horton Conway",
            "Harold Davenport",
            "John Edensor Littlewood",
            "Ernest Barnes",
            "W. W. Rouse Ball"],
            "http://example.org/conway_academic_lineage")

        self.assertEqual('W. W. Rouse Ball',
                         cut.rest().rest().rest().rest().first())

    def test_make_no_ident(self):
        cut = List.from_sequence(["John Horton Conway",
            "Harold Davenport",
            "John Edensor Littlewood",
            "Ernest Barnes",
            "W. W. Rouse Ball"])

        self.assertIsNone(cut.rest())

    def test_make_no_ident_for_query_match(self):
        self.ctx.List.from_sequence(["John Horton Conway",
            "Harold Davenport",
            "John Edensor Littlewood",
            "Ernest Barnes",
            "W. W. Rouse Ball"],
            "http://example.org/conway_academic_lineage")
        self.context.save()

        cut = self.context.stored(List).from_sequence(["John Horton Conway",
            "Harold Davenport",
            "John Edensor Littlewood",
            "Ernest Barnes",
            "W. W. Rouse Ball"])

        for m in cut.load():
            self.assertEqual(URIRef("http://example.org/conway_academic_lineage"),
                    m.identifier)

    def test_make_no_ident_for_query_no_match(self):
        self.ctx.List.from_sequence(["John Horton Conway",
            "Harold Davenport",
            "John Edensor Littlewood",
            "Ernest Barnes",
            "W. W. Rouse Ball"],
            "http://example.org/conway_academic_lineage")
        self.context.save()

        cut = self.context.stored(List).from_sequence(["John Horton Conway",
            "Harold Davenport",
            "John Edensor Littlewood",
            "Darren Bell",
            "W. W. Rouse Ball"])

        for m in cut.load():
            self.fail(f'Expected no matches. Got: {m}')


class ListLoadDataObjectSequencesTest(_DataTest):
    ctx_classes = (List,)

    def test_basic(self):
        lst = ["John Horton Conway",
               "Harold Davenport",
               "John Edensor Littlewood",
               "Ernest Barnes",
               "W. W. Rouse Ball"]
        self.ctx.List.from_sequence(lst, "http://example.org/conway_academic_lineage")
        self.context.save()

        cut = self.context.stored(List)(ident="http://example.org/conway_academic_lineage")

        hit = False
        for seq in cut.load_dataobject_sequences():
            hit = True
            assert lst == [x.first() for x in seq]

        self.assertTrue(hit, 'Should have emitted at least one sequence')

    def test_split(self):
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')
        c = self.ctx.List(key='c', first='c')
        d = self.ctx.List(key='d', first='d')
        nil = self.ctx.List.nil

        a.rest(b)
        b.rest(c)
        c.rest(nil)
        self.context.save()

        b.rest(d)
        d.rest(nil)
        self.context.save()

        for t in self.context.rdf:
            print(' '.join(x.n3() for x in t))
        query = self.context.stored(List)(key='a')
        result = list(query.load_sequences())
        assert ['a', 'b', 'c'] in result
        assert ['a', 'b', 'd'] in result

    def test_loop_single(self):
        a = self.ctx.List(key='a', first='a')

        a.rest(a)
        self.context.save()

        query = self.context.stored(List)(key='a')
        hit = False
        for seq in query.load_sequences():
            hit = True
            assert list(islice(seq, 5)) == ['a', 'a', 'a', 'a', 'a']
        self.assertTrue(hit, 'Expected a sequence')

    def test_loop_double(self):
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')

        a.rest(b)
        b.rest(a)
        self.context.save()

        query = self.context.stored(List)(key='a')
        hit = False
        for seq in query.load_sequences():
            hit = True
            assert list(islice(seq, 5)) == ['a', 'b', 'a', 'b', 'a']
        self.assertTrue(hit, 'Expected a sequence')

    def test_loop_triple(self):
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')
        c = self.ctx.List(key='c', first='c')

        a.rest(b)
        b.rest(c)
        c.rest(a)
        self.context.save()

        query = self.context.stored(List)(key='a')
        hit = False
        for seq in query.load_sequences():
            hit = True
            assert list(islice(seq, 6)) == ['a', 'b', 'c', 'a', 'b', 'c']
        self.assertTrue(hit, 'Expected a sequence')

    def test_partial_loop(self):
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')
        c = self.ctx.List(key='c', first='c')

        a.rest(b)
        b.rest(c)
        c.rest(b)
        self.context.save()

        query = self.context.stored(List)(key='a')
        hit = False
        for seq in query.load_sequences():
            hit = True
            assert list(islice(seq, 6)) == ['a', 'b', 'c', 'b', 'c', 'b']
        self.assertTrue(hit, 'Expected a sequence')


class ListLoadDataObjectSequencesImproperTerminationTest(_DataTest):
    ctx_classes = (List,)

    def setUp(self):
        super().setUp()
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')
        c = self.ctx.List(key='c', first='c')

        a.rest(b)
        b.rest(c)
        self.context.save()

    def test_improperly_terminated_assumes_nil(self):
        query = self.context.stored(List)(key='a')
        assert ['a', 'b', 'c'] in list(query.load_sequences())

    def test_improperly_terminated_warns(self):
        query = self.context.stored(List)(key='a')
        with captured_logging() as logs:
            list(query.load_sequences())
            self.assertRegexpMatches(logs.getvalue(), r'not properly terminated')

    def test_nil_sequences_empty(self):
        assert list(self.context.stored(List).nil.load_sequences()) == [[]]

    def test_nil_sequences_not_improperly_terminated(self):
        with captured_logging() as logs:
            list(self.context.stored(List).nil.load_sequences())
            self.assertNotRegexpMatches(logs.getvalue(), r'not properly terminated')
