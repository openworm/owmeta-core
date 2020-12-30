import rdflib as R
from .utils import FCN
from .rdf_utils import UP, DOWN


class ZeroOrMore(object):
    def __init__(self, identifier, predicate, direction=DOWN):
        self.identifier = identifier
        self.predicate = predicate
        self.direction = direction

    def __repr__(self):
        return "{}({}, {}, {})".format(FCN(type(self)),
                                       repr(self.identifier),
                                       repr(self.predicate),
                                       repr(self.direction))


class SubClassModifier(ZeroOrMore):

    def __init__(self, rdf_type):
        super(SubClassModifier, self).__init__(rdf_type, R.RDFS.subClassOf, UP)

    def __repr__(self):
        return FCN(type(self)) + '(' + repr(self.identifier) + ')'


class ZeroOrMore2(object):
    def __init__(self, identifier, predicate, index, direction=DOWN):
        self.identifier = identifier
        self.predicate = predicate
        self.direction = direction
        self.direction = direction
        self.index = index

    def __repr__(self):
        return "{}({}, {}, {})".format(FCN(type(self)),
                                       repr(self.identifier),
                                       repr(self.predicate),
                                       repr(self.direction))


class SubPropertyOfModifier(ZeroOrMore2):

    def __init__(self, rdf_property):
        super(SubPropertyOfModifier, self).__init__(rdf_property, R.RDFS.subPropertyOf, 1, direction=UP)

    def __repr__(self):
        return FCN(type(self)) + '(' + repr(self.identifier) + ')'
