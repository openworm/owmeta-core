from rdflib.term import URIRef
from .dataobject import DataObject, ObjectProperty, This
from .context_common import CONTEXT_IMPORTS
from .mapper import mapped


# ContextDataObject was moved to a separate module from Context to avoid a dependency
# cycle with dataobject

@mapped
class ContextDataObject(DataObject):
    """ Represents a context """
    class_context = 'http://openworm.org/schema'
    rdf_type = URIRef('http://openworm.org/schema/Context')
    imports = ObjectProperty(value_type=This,
                             multiple=True,
                             link=CONTEXT_IMPORTS)
