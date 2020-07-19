from owmeta_core.dataobject import DataObject, DatatypeProperty
from owmeta_core.mapper import mapped


@mapped
class Person(DataObject):
    class_context = 'http://example.org/Person'
    first_name = DatatypeProperty()
    last_name = DatatypeProperty()


def owm_data(ns):
    print(ns.context, Person.definition_context)
    ns.context.add_import(Person.definition_context)
    ns.context(Person)(key='mevers',
            first_name='Medgar',
            last_name='Evers')
