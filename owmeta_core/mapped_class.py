from __future__ import print_function

import logging
import importlib as IM

import six
import rdflib as R

from .context_mapped_class_util import find_base_namespace

L = logging.getLogger(__name__)


class MappedClass(type):

    """A type for MappedClasses

    Sets up the graph with things needed for MappedClasses
    """
    def __init__(self, name, bases, dct):
        L.debug("INITIALIZING %s", name)
        super(MappedClass, self).__init__(name, bases, dct)

        self.__base_namespace = find_base_namespace(dct, bases)

        self.__rdf_type = None
        # Set the rdf_type early
        if 'rdf_type' in dct:
            self.__rdf_type = R.URIRef(dct['rdf_type'])

        if self.__rdf_type is None:
            self.__rdf_type = self.base_namespace[self.__name__]

        self.__rdf_namespace = None
        rdf_ns = dct.get('rdf_namespace', None)
        if rdf_ns is not None:
            L.debug("Setting rdf_namespace to {}".format(rdf_ns))
            if not isinstance(rdf_ns, R.Namespace):
                rdf_ns = R.Namespace(rdf_ns)
            self.__rdf_namespace = rdf_ns

        self.__schema_namespace = None
        schema_ns = dct.get('schema_namespace', None)
        if schema_ns is not None:
            L.debug("Setting schema_namespace to {}".format(schema_ns))
            if not isinstance(schema_ns, R.Namespace):
                schema_ns = R.Namespace(schema_ns)
            self.__schema_namespace = schema_ns

        if self.__rdf_namespace is None:
            if self.base_data_namespace is not None:
                rdf_namespace = self.base_data_namespace[self.__name__] + '#'
            else:
                rdf_namespace = self.base_namespace[self.__name__] + '#'
            L.debug("Setting rdf_namespace to {}".format(rdf_namespace))
            self.__rdf_namespace = R.Namespace(rdf_namespace)

        if self.__schema_namespace is None:
            L.debug("Setting schema_namespace to {}".format(self.base_namespace[self.__name__] + '/'))
            self.__schema_namespace = R.Namespace(
                self.base_namespace[self.__name__] + "/")

        self.__rdf_type_object_callback = dct.get('rdf_type_object_callback')
        self.__rdf_type_object = dct.get('rdf_type_object')

        if not getattr(self, 'unmapped', False) and not dct.get('unmapped'):
            self.register_on_module()

    def register_on_module(self, module=None):
        '''
        "Registers" this class on a module (typically the one in which the class is
        defined) such that owmeta-core functions can locate it. This happens automatically
        when the class is defined unless the 'unmapped' attribute is defined and set to
        `True`.

        This mechanism necessary in some cases where classes are generated dynamically or
        in a method and aren't necessarily assigned to attributes on the module where they
        are defined.
        '''
        module = module or IM.import_module(self.__module__)
        if not hasattr(module, '__yarom_mapped_classes__'):
            module.__yarom_mapped_classes__ = [self]
        else:
            module.__yarom_mapped_classes__.append(self)

    @property
    def base_namespace(self):
        return self.__base_namespace

    @property
    def rdf_type_object(self):
        if self.__rdf_type_object_callback is not None:
            rdto = self.__rdf_type_object_callback()
            if rdto is not None:
                self.__rdf_type_object_callback = None
                self.__rdf_type_object = rdto
        return self.__rdf_type_object

    @rdf_type_object.setter
    def rdf_type_object(self, value):
        if value is not None:
            self.__rdf_type_object_callback = None
        self.__rdf_type_object = value

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

    @property
    def schema_namespace(self):
        return self.__schema_namespace

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
        return self
