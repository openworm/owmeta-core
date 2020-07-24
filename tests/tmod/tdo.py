from owmeta_core.dataobject import DataObject, DatatypeProperty


class TDO(DataObject):
    rdf_type = 'http://openworm.org/entities/TDO'
    a = DatatypeProperty()
