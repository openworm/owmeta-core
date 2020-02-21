from __future__ import print_function

import logging

import six
import rdflib as R

L = logging.getLogger(__name__)


class MappedClass(type):

    """A type for MappedClasses

    Sets up the graph with things needed for MappedClasses
    """
    def __init__(self, name, bases, dct):
        L.debug("INITIALIZING %s", name)
        super(MappedClass, self).__init__(name, bases, dct)
        if 'auto_mapped' in dct:
            self.mapped = True
        else:
            self.mapped = False

        self.__rdf_type = None
        # Set the rdf_type early
        if 'rdf_type' in dct:
            self.__rdf_type = dct['rdf_type']

        if self.__rdf_type is None:
            self.__rdf_type = self.base_namespace[self.__name__]

        self.__rdf_namespace = None
        rdf_ns = dct.get('rdf_namespace', None)
        if rdf_ns is not None:
            L.debug("Setting rdf_namespace to {}".format(rdf_ns))
            self.__rdf_namespace = rdf_ns

        if self.__rdf_namespace is None:
            L.debug("Setting rdf_namespace to {}".format(self.base_namespace[self.__name__]))
            self.__rdf_namespace = R.Namespace(
                self.base_namespace[self.__name__] + "/")

    @property
    def rdf_type(self):
        return self.__rdf_type

    @rdf_type.setter
    def rdf_type(self, new_type):
        if not isinstance(new_type, R.URIRef) and \
                isinstance(new_type, (str, six.text_type)):
            new_type = R.URIRef(new_type)
        self.__rdf_type = new_type

    @property
    def rdf_namespace(self):
        return self.__rdf_namespace

    def __lt__(self, other):
        res = False
        if issubclass(other, self) and not issubclass(self, other):
            res = True
        elif issubclass(self, other) == issubclass(other, self):
            res = self.__name__ < other.__name__
        return res

    def on_mapper_add_class(self, mapper):
        """ Called by :class:`owmeta_core.mapper.Mapper`

        Registers certain properties of the class
        """
        self.mapper = mapper
        L.debug("REGISTERING %s", self.__name__)

        return self

    def after_mapper_module_load(self, mapper):
        """ Called after all classes in a module have been loaded """