from owmeta_core.dataobject import DataObject, DatatypeProperty


class Monkey(DataObject):
    class_context = 'http://example.org/primate/monkey'

    bananas = DatatypeProperty()

    def identifier_augment(self):
        return type(self).rdf_namespace['paul']

    def defined_augment(self):
        return True


class Giraffe(DataObject):
    class_context = 'http://example.org/ungulate/giraffe'


def owm_data(ns):
    ns.context.add_import(Monkey.definition_context)
    ns.context.add_import(Giraffe.definition_context)
