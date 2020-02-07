from json import loads, dumps

from rdflib.term import Literal, bind, Identifier, URIRef
import six

from .quantity import Quantity
from .utils import FCN
from . import BASE_SCHEMA_URL
bind(URIRef(BASE_SCHEMA_URL + '/datatype/quantity'),
     Quantity,
     constructor=Quantity.parse,
     lexicalizer=Quantity.__str__)

bind(URIRef(BASE_SCHEMA_URL + '/datatype/list'),
     list,
     constructor=loads,
     lexicalizer=dumps)
bind(URIRef(BASE_SCHEMA_URL + '/datatype/object'),
     dict,
     constructor=loads,
     lexicalizer=lambda x: dumps(x, sort_keys=True))

# XXX: RDFLib should take the first match for a python type, so we'll translate into the
# list and object types above
bind(URIRef('http://markw.cc/yarom/schema/datatype/list'),
     list,
     constructor=loads,
     lexicalizer=dumps)
bind(URIRef('http://markw.cc/yarom/schema/datatype/object'),
     dict,
     constructor=loads,
     lexicalizer=lambda x: dumps(x, sort_keys=True))


class PropertyValue(object):
    """ Holds a literal value for a property """

    # Made to look like a GraphObject, but not subclassing so we can have slots

    __slots__ = ('value', 'properties', 'owner_properties')

    def __init__(self, value):
        self.properties = []
        self.owner_properties = []
        if not isinstance(value, Identifier):
            self.value = Literal(value)
        else:
            self.value = value

    def triples(self, *args, **kwargs):
        return []

    @property
    def identifier(self):
        return self.value

    @property
    def defined(self):
        return True

    @property
    def idl(self):
        return self.identifier

    def __hash__(self):
        return hash(self.value)

    def __str__(self):
        if six.PY3:
            return "PV("+self.value+")"
        else:
            return "PV("+self.value.encode('UTF-8')+")"

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.value))

    def __lt__(self, other):
        return self.value < other.value

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif isinstance(other, PropertyValue):
            return self.value == other.value
        else:
            return False
