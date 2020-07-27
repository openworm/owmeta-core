from collections import namedtuple
from itertools import cycle, chain
import logging

from rdflib.term import URIRef
from rdflib.namespace import RDF

from . import BASE_SCHEMA_URL
from .dataobject import (BaseDataObject,
                         DataObject,
                         This,
                         ObjectProperty,
                         DatatypeProperty,
                         UnionProperty,
                         Alias)


L = logging.getLogger(__name__)


class Bag(DataObject):

    """
    A convenience class for working with a collection of objects

    Example::

        v = Bag('unc-13 neurons and muscles')
        n = P.Neuron()
        m = P.Muscle()
        n.receptor('UNC-13')
        m.receptor('UNC-13')
        for x in n.load():
            v.value(x)
        for x in m.load():
            v.value(x)
        # Save the group for later use
        v.save()
        ...
        # get the list back
        u = Bag('unc-13 neurons and muscles')
        nm = list(u.value())
    """

    class_context = BASE_SCHEMA_URL

    value = UnionProperty()
    '''An object in the group'''

    add = Alias(value)
    '''An alias for `value`'''

    name = DatatypeProperty()
    '''The name of the group of objects'''

    group_name = Alias(name)
    '''Alias for `name`'''

    def defined_augment(self):
        return self.group_name.has_defined_value()

    def identifier_augment(self):
        return self.make_identifier_direct(self.group_name.onedef())


class List(BaseDataObject):
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

    def load_dataobject_sequences(self, seen=None):
        '''
        Loads the sequences of `rest` values starting from this node.

        If this node is undefined, then this method generates *all* lists, including
        sub-lists, in the configured RDF graph. Also, there is no guarantee that there is
        just *one* list starting from this node.
        '''
        if seen is None:
            seen = list()

        if self.idl == type(self).nil.identifier:
            yield []
            return

        if self.identifier in seen:
            # Maybe a loop was made on purpose, so no warning, but still worth noting.
            L.info('Loop detected: %s in %s', self, seen)
            yield _Loop((), self)
            return

        for m in self.load():
            rests = m.rest.get()

            seen.append(m.identifier)

            hit = False
            for rest in rests:
                for rest_lst in rest.load_dataobject_sequences(seen):
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
