from owmeta_core.dataobject import DataObject, DatatypeProperty


class Donkey(DataObject):
    base_namespace = 'http://example.org/schema/'
    class_context = 'http://example.org/ungulate/donkey'

    bananas = DatatypeProperty()

    def identifier_augment(self):
        return type(self).rdf_namespace['danny']

    def defined_augment(self):
        return True
