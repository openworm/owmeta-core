from collections import namedtuple
from itertools import cycle, chain
import logging
import re

from rdflib.term import URIRef
from rdflib.namespace import RDF, RDFS

from . import RDF_CONTEXT, RDFS_CONTEXT
from .dataobject import (BaseDataObject,
                         This,
                         ObjectProperty,
                         UnionProperty)
from .dataobject_property import UnionProperty as UnionPropertyType


L = logging.getLogger(__name__)

CONTAINER_MEMBERSHIP_PROPERTY_RE = re.compile(r'^_([1-9]+[0-9]*)$')


class Container(BaseDataObject):
    '''
    Base class for rdfs:Containers

    Example (`Bag`, `Alt`, and `Seq` have the same operations)::

        >>> nums = Bag(ident="http://example.org/fav-numbers")
        >>> nums[1] = 42
        >>> nums.set_member(2, 415)
        owmeta_core.statement.Statement(...)
        >>> nums._3(15)
        owmeta_core.statement.Statement(...)
        >>> nums._2.index
        2
        >>> nums._1()
        42
        >>> nums[2]
        415
        >>> nums._2(6)
        owmeta_core.statement.Statement(...)
        >>> nums[2]
        6

    Note that because the set of entries in ``rdfs:Container`` is not bounded, iteration
    over `Containers <Container>` is not bounded. To iterate over a `Container`, it is
    recommended to add some external bound with `itertools.islice` or something like
    ``zip(range(bound), container)``. Where values have not been set, `None` will be
    returned.
    '''
    rdf_type = RDFS.Container
    class_context = RDFS_CONTEXT

    def __getitem__(self, index):
        prop = getattr(self, f'_{index}', None)
        if prop is None:
            return None
        item_to_return = None
        extra_items = None
        for item in prop.get():
            if item_to_return is None:
                item_to_return = item
            elif item_to_return == item:
                pass
            elif extra_items is None:
                extra_items = [item_to_return, item]
            else:
                extra_items.append(item)
        if extra_items:
            # Unlike regular Property access, there's generally not a presumption that
            # one of many values can be selected arbitrarily. Also, an iteration that
            # sometimes does what you expect and sometimes doesn't is really frustrating.
            raise ContainerValueConflict(index, extra_items)
        return item_to_return

    def __getattr__(self, name):
        md = CONTAINER_MEMBERSHIP_PROPERTY_RE.match(name)
        if md:
            try:
                prop = super().__getattribute__(name)
            except AttributeError:
                prop = None
            if prop is None:
                prop = self.attach_property(ContainerMembershipProperty, index=int(md.group(1)))
            return prop
        raise AttributeError(name)

    def __setitem__(self, index, item):
        self.set_member(index, item)

    def set_member(self, index, item):
        '''
        Set a member at the given index.

        If an existing value is set at the given index, then it will be replaced. Note
        that, as described in the `RDF Primer`_, there is no well-formedness guarantee: in
        particular, some other instance of a container may declare a different value at
        the same index.

        .. _RDF Primer: https://www.w3.org/TR/rdf-primer/#collections
        '''
        prop = getattr(self, f'_{index}', None)
        if isinstance(prop, ContainerMembershipProperty):
            return prop(item)
        raise Exception(f'Non-ContainerMembershipProperty set at _{index}')


class ContainerValueConflict(Exception):
    def __init__(self, index, items):
        super().__init__(f'More than one item is declared at index {index}. Items: {items!r}')
        self.index = index
        self.items = items


class ContainerMembershipProperty(UnionPropertyType):
    '''
    Base class for container membership properties like ``rdf:_1``, ``rdf:_2``, ...
    '''
    class_context = RDFS_CONTEXT
    owner_type = BaseDataObject
    rdf_type = RDFS.ContainerMembershipProperty

    def __init__(self, index, **kwargs):
        super().__init__(**kwargs)
        if isinstance(index, str):
            md = CONTAINER_MEMBERSHIP_PROPERTY_RE.match(index)
            if not md:
                raise ValueError(f'Expected an integer > 0. Received {index!r}.')
            index = int(md.group(1))
        elif isinstance(index, int):
            if index <= 0:
                raise ValueError('Expected an integer > 0')
        else:
            raise ValueError('Expected an integer > 0')

        self.__index = index
        name = f'_{index}'
        try:
            self.link = RDF[name]
        except KeyError as e:
            raise ValueError('Expected an integer > 0') from e
        self.linkName = name
        # We need to add the (..., rdf:type, rdfs:ContainerMembershipProperty) triples to
        # do proper entailment, ultimately of the rdfs:subPropertyOf(rdfs:member)
        # relationship.
        type(self).rdf_type_class.contextualize(self.context)(ident=self.link)

    @property
    def index(self):
        return self.__index


class Bag(Container):
    """
    A convenience class for working with a rdf:Bag
    """
    rdf_type = RDF.Bag
    class_context = RDF_CONTEXT


class Alt(Container):
    rdf_type = RDF.Alt
    class_context = RDF_CONTEXT


class Seq(Container):
    rdf_type = RDF.Seq
    class_context = RDF_CONTEXT


class List(BaseDataObject):
    class_context = RDF_CONTEXT
    rdf_type = RDF.List
    first = UnionProperty(link=RDF.first)
    rest = ObjectProperty(link=RDF.rest, value_type=This)

    @classmethod
    def from_sequence(cls, sequence, ident=None):
        first = cls.nil
        last = None
        for i, s in enumerate(sequence):
            this = cls(first=s)
            if first is cls.nil:
                first = this
                if ident:
                    this.identifier = URIRef(ident)
            else:
                if ident:
                    this.identifier = URIRef(ident + f"#_{i}")
            if last is not None:
                last.rest(this)
            last = this
        last.rest(cls.nil)

        return first

    def load_dataobject_sequences(self):
        '''
        Loads the sequences of `rest` values starting from this node.

        If this node is undefined, then this method generates *all* lists, including
        sub-lists, in the configured RDF graph. Also, there is no guarantee that there is
        just *one* list starting from this node.
        '''
        return self._load_dataobject_sequences()

    def _load_dataobject_sequences(self, seen=None):
        if seen is None:
            seen = list()

        if self.idl == type(self).nil.identifier:
            yield []
            return

        for m in self.load():
            rests = m.rest.get()

            if m.identifier in seen:
                # Maybe a loop was made on purpose, so no warning, but still worth noting.
                L.info('Loop detected: %s in %s', self, seen)
                yield _Loop((), m)

                # We can drop here since there's only going to be one result except for
                # when we initially loaded from something without an identifier, but if
                # there's something in `seen`, then we've already passed that case. You
                # *could* pass something in to `seen` on the initial call, but that isn't
                # a part of the *public* interface
                return

            seen.append(m.identifier)

            hit = False
            for rest in rests:
                for rest_lst in rest._load_dataobject_sequences(seen):
                    hit = True
                    if isinstance(rest_lst, _Loop):
                        if rest_lst.loop.identifier == m.identifier:
                            yield cycle((m,) + rest_lst.parts)
                        else:
                            yield _Loop((m,) + rest_lst.parts, rest_lst.loop)
                    elif isinstance(rest_lst, (chain, cycle)):
                        yield chain((m,), rest_lst)
                    else:
                        yield [m] + rest_lst
            seen.pop()
            if not hit:
                L.warning('List %s was not properly terminated', m)
                yield [m]

    def load_sequences(self):
        for m in self.load_dataobject_sequences():
            if isinstance(m, list):
                yield [x.first() for x in m]
            else:
                yield (x.first() for x in m)


_Loop = namedtuple('Loop', ('parts', 'loop'))


List.nil = List.definition_context(List)(ident=RDF.nil)
