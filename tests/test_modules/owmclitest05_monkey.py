from owmeta_core.dataobject import DataObject, DatatypeProperty


class Monkey(DataObject):
    base_namespace = 'http://example.org/schema/'
    class_context = 'http://example.org/primate/monkey'

    bananas = DatatypeProperty()

    def identifier_augment(self):
        return type(self).rdf_namespace['paul']

    def defined_augment(self):
        return True
