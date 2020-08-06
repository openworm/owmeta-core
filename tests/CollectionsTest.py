from __future__ import absolute_import

from itertools import islice

from rdflib.term import URIRef
from rdflib.namespace import RDF

from owmeta_core.collections import (Bag, Seq, Alt, List, ContainerMembershipProperty,
                                     ContainerValueConflict)

from .DataTestTemplate import _DataTest
from .TestUtilities import captured_logging


class _ContainerTestBase(object):
    container_type = None

    def setUp(self):
        super().setUp()
        self.cut = self.container_type(ident="http://example.org/fav-numbers")

    def test_set_member(self):
        nums = self.cut
        nums.set_member(1, 42)
        nums.set_member(2, 5222)
        nums.set_member(3, 415)
        assert nums._3() == 415

    def test_set_getitem(self):
        nums = self.cut
        nums.set_member(1, 42)
        assert nums[1] == 42

    def test_getitem_on_unset(self):
        nums = self.cut
        assert nums[1] is None

    def test_get_unset_membership_attribute(self):
        nums = self.cut
        nums._5(8)
        assert nums._5() == 8

    def test_iter(self):
        nums = self.cut
        nums._5(8)
        assert list(islice(nums, 1, 6)) == [None, None, None, None, 8]

    def test_container_value_conflict(self):
        nums = self.context(self.container_type)(ident="http://example.org/fav-numbers")
        nums._1(8)
        self.context.save()
        nums._1(4)
        self.context.save()
        nums0 = self.context.stored(self.container_type)(ident="http://example.org/fav-numbers")
        with self.assertRaises(ContainerValueConflict):
            nums0[1]

    def test_auto_prop_sameas(self):
        nums = self.cut
        prop = nums.set_member(2, 12).property
        self.assertIs(prop, nums._2)

    def test_index(self):
        nums = self.cut
        nums.set_member(2, 12).property
        self.assertEqual(2, nums._2.index)


class AltTest(_ContainerTestBase, _DataTest):
    container_type = Alt


class BagTest(_ContainerTestBase, _DataTest):
    container_type = Bag


class SeqTest(_ContainerTestBase, _DataTest):
    container_type = Seq


class ContainerMembershipPropertyTest(_DataTest):
    def test_index(self):
        assert ContainerMembershipProperty(index=3, owner=None, resolver=None).index == 3

    def test_link(self):
        cut = ContainerMembershipProperty(index=3, owner=None, resolver=None)
        assert cut.link == RDF._3

    def test_invalid_index(self):
        with self.assertRaises(ValueError):
            ContainerMembershipProperty(index=0, owner=None, resolver=None)

    def test_invalid_non_int(self):
        with self.assertRaises(ValueError):
            ContainerMembershipProperty(index='not a number', owner=None, resolver=None)


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

    def test_loop_split(self):
        '''
        ```
        a->b->c->b[loop]
            ->d->b[loop]
        ```
        '''
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')
        c = self.ctx.List(key='c', first='c')
        d = self.ctx.List(key='d', first='d')

        a.rest(b)
        b.rest(c)
        c.rest(b)
        self.context.save()

        b.rest(d)
        d.rest(b)
        self.context.save()

        query = self.context.stored(List)(key='a')
        seqs = set([tuple(islice(seq, 6)) for seq in query.load_sequences()])
        assert seqs == set([('a', 'b', 'c', 'b', 'c', 'b'),
                            ('a', 'b', 'd', 'b', 'd', 'b')])

    def test_loop_split_long(self):
        '''
        ```
        a->b->c->d->c[loop]
            ->c->b[loop]
            ->d->c->d[loop]
            ->d->c->b[loop]
        ```
        '''
        a = self.ctx.List(key='a', first='a')
        b = self.ctx.List(key='b', first='b')
        c = self.ctx.List(key='c', first='c')
        d = self.ctx.List(key='d', first='d')

        a.rest(b)
        b.rest(c)
        c.rest(d)
        d.rest(c)
        self.context.save()

        c.rest(b)
        b.rest(d)
        self.context.save()

        query = self.context.stored(List)(key='a')
        seqs = set([tuple(islice(seq, 7)) for seq in query.load_sequences()])
        assert seqs == set([('a', 'b', 'c', 'b', 'c', 'b', 'c'),
                            ('a', 'b', 'c', 'd', 'c', 'd', 'c'),
                            ('a', 'b', 'd', 'c', 'd', 'c', 'd'),
                            ('a', 'b', 'd', 'c', 'b', 'd', 'c')])


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
