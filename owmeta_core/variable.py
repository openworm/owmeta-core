import rdflib
from .utils import FCN
from .graph_object import GraphObject, IdentifierMissingException


class Variable(GraphObject):
    '''
    A graph object representing a variable. Typically used in property values
    '''
    def __init__(self, name, **kwargs):
        super(Variable, self).__init__()
        self.var = rdflib.Variable(name)

    @property
    def identifier(self):
        raise IdentifierMissingException(self)

    def variable(self):
        return self.var

    @property
    def defined(self):
        return False

    def __hash__(self):
        return hash(self.var)

    def __str__(self):
        return str(self.var)

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.var))
