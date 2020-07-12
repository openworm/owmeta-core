from . import BASE_CONTEXT
from .dataobject import DataObject, ObjectProperty, This
from .context_common import CONTEXT_IMPORTS
from .mapper import mapped


# ContextDataObject was moved to a separate module from Context to avoid a dependency
# cycle with dataobject

@mapped
class ContextDataObject(DataObject):
    """ Represents a context """
    class_context = BASE_CONTEXT
    rdf_type = BASE_CONTEXT.base_namespace['Context']
    imports = ObjectProperty(value_type=This,
                             multiple=True,
                             link=CONTEXT_IMPORTS)
