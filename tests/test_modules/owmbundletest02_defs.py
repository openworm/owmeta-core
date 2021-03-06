from owmeta_core.dataobject import DataObject, DatatypeProperty


class Person(DataObject):
    class_context = 'http://example.org/Person'
    first_name = DatatypeProperty()
    last_name = DatatypeProperty()


def owm_data(ns):
    print(ns.context, Person.definition_context)
    ns.context.add_import(Person.definition_context)
    ns.context(Person)(ident='http://example.org/people/mevers',
            first_name='Medgar',
            last_name='Evers')
