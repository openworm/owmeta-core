from owmeta_core.dataobject import DataObject, DatatypeProperty
from owmeta_core.mapper import mapped


@mapped
class Person(DataObject):
    class_context = 'http://example.org/Person'
    first_name = DatatypeProperty()
    last_name = DatatypeProperty()
