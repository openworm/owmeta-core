from __future__ import print_function
from functools import partial
import hashlib
import importlib as IM
import logging

import rdflib as R
from rdflib.term import URIRef
import six

from . import BASE_DATA_URL, BASE_SCHEMA_URL, DEF_CTX, RDF_CONTEXT
from .contextualize import (Contextualizable,
                            ContextualizableClass,
                            contextualize_helper,
                            decontextualize_helper)
from .context import ContextualizableDataUserMixin, ClassContext, Context
from .context_mapped_class_util import find_class_context

from .graph_object import (GraphObject,
                           ComponentTripler,
                           GraphObjectQuerier,
                           IdentifierMissingException)
from .rdf_utils import triples_to_bgp, deserialize_rdflib_term
from .identifier_mixin import IdMixin
from .inverse_property import InverseProperty
from .mapped_class import MappedClass
from .rdf_type_resolver import RDFTypeResolver
from .rdf_query_util import (goq_hop_scorer,
                             get_most_specific_rdf_type,
                             oid,
                             load,
                             load_terms)
from .utils import FCN

import owmeta_core.dataobject_property as SP

__all__ = [
    "BaseDataObject",
    "ContextMappedClass",
    "DataObject"]

L = logging.getLogger(__name__)


PropertyTypes = dict()

This = object()
""" A reference to be used in class-level property declarations to denote the
    class currently being defined. For example::

        >>> class Person(DataObject):
        ...     parent = ObjectProperty(value_type=This,
        ...                             inverse_of=(This, 'child'))
        ...     child = ObjectProperty(value_type=This)
"""


DATAOBJECT_PROPERTY_NAME_PREFIX = '_owm_'
'''
Prefix for property attribute names
'''


class PropertyProperty(Contextualizable, property):
    def __init__(self, cls=None, *args, cls_thunk=None):
        super(PropertyProperty, self).__init__(*args)
        self._cls = cls
        self._cls_thunk = cls_thunk
        self._super_init_args = args
        if cls and cls.__doc__:
            self.__doc__ = cls.__doc__

    def contextualize_augment(self, context):
        if self._cls is None:
            self._cls = self._cls_thunk()
        return type(self)(self._cls.contextualize_class(context),
                          *self._super_init_args)

    @property
    def property(self):
        if self._cls is None:
            self._cls = self._cls_thunk()
        return self._cls

    def __call__(self, dataobject):
        '''
        Attach this property to the given `.DataObject`
        '''
        for p in dataobject.properties:
            if isinstance(p, self.property):
                return p
        return dataobject.attach_property(self.property, ephemeral=True)

    def __getattr__(self, attr):
        # Provide a weak sort of proxying to the class we're holding
        cls = object.__getattribute__(self, '_cls')
        if cls is None:
            cls = self._cls_thunk()
            self._cls = cls
        return getattr(cls, attr)

    def __repr__(self):
        return '{}(cls={})'.format(FCN(type(self)), repr(self._cls))


def mp(c, k):
    ak = DATAOBJECT_PROPERTY_NAME_PREFIX + k
    if c.lazy:
        def getter(target):
            attr = getattr(target, ak, None)
            if attr is None:
                attr = target.attach_property(c, name=ak)
            return attr
    else:
        def getter(target):
            return getattr(target, ak)

    return PropertyProperty(c, getter)


class PThunk(object):
    def __init__(self):
        self.result = None

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()


class CPThunk(PThunk):
    def __init__(self, c):
        super(CPThunk, self).__init__()
        self.c = c

    def __call__(self, *args, **kwargs):
        self.result = self.c
        return self.c


class APThunk(PThunk):
    def __init__(self, t, args, kwargs):
        super(APThunk, self).__init__()
        self.t = t
        self.args = args
        self.kwargs = kwargs

    def __call__(self, cls, linkName):
        if self.result is None:
            if 'linkName' in self.kwargs:
                linkName = self.kwargs.pop('linkName')
            self.result = cls._create_property_class(linkName,
                                                     *self.args,
                                                     property_type=self.t,
                                                     **self.kwargs)
        return self.result

    def __repr__(self):
        return '{}({}{})'.format(self.t, self.args and ',\n'.join(self.args) + ', ' or '',
                                 ', '.join(k + '=' + str(v) for k, v in self.kwargs.items()))


class Alias(object):
    '''
    Used to declare that a descriptor is an alias to some other
    `~dataobject_property.Property`

    Example usage::

        class Person(DataObject):
            child = DatatypeProperty()
            offspring = Alias(child)
    '''
    def __init__(self, target):
        '''
        Parameters
        ----------
        target : dataobject_property.Property
            The property to alias
        '''
        self.target = target

    def __repr__(self):
        return 'Alias(' + repr(self.target) + ')'


def DatatypeProperty(*args, **kwargs):
    '''
    Used in a `.DataObject` implementation to designate a property whose values are
    not `DataObjects <.DataObject>`.

    An example `DatatypeProperty` use::

        class Person(DataObject):
            name = DatatypeProperty()
            age = DatatypeProperty()

        Person(name='Abioye', age=34)
    '''
    return APThunk('DatatypeProperty', args, kwargs)


def ObjectProperty(*args, **kwargs):
    '''
    Used in a `.DataObject` implementation to designate a property whose values are other
    `DataObjects <.DataObject>`.

    An example `ObjectProperty` use::

        class Person(DataObject):
            name = DatatypeProperty()
            friend = ObjectProperty()

        Person(name='Abioye', friend=Person(name='Baako'))
    '''
    return APThunk('ObjectProperty', args, kwargs)


def UnionProperty(*args, **kwargs):
    '''
    Used in a `.DataObject` implementation to designate a property whose values are either other
    `DataObjects <.DataObject>` or literals (e.g., str, int).

    An example `UnionProperty` use::

        class Address(DataObject):
            street = DatatypeProperty()
            number = DatatypeProperty()
            city = DatatypeProperty()
            state = DatatypeProperty()
            zip = DatatypeProperty()

        class Person(DataObject):
            name = DatatypeProperty()
            address = UnionProperty()

        Person(name='Umoja', address='38 West 88th Street, Manhattan NY 10024 , New York, USA')
        Person(name='Umoja', address=Address(number=38,
                                             street='West 88th Street',
                                             city='New York',
                                             state='NY',
                                             zip=10024))
    '''
    return APThunk('UnionProperty', args, kwargs)


def _get_rdf_type_property():
    return RDFTypeProperty


class ContextMappedClass(MappedClass, ContextualizableClass):
    '''
    The metaclass for a `BaseDataObject`.
    '''

    context_carries = ('rdf_type',
                       'rdf_namespace',
                       'schema_namespace',
                       'rdf_type_object_deferred',
                       'rdf_type_object')

    rdf_type_object_deferred = False

    def __init__(self, name, bases, dct):
        super(ContextMappedClass, self).__init__(name, bases, dct)

        self.rdf_type_object_deferred = dct.get('rdf_type_object_deferred', False)

        ctx = find_class_context(self, dct, bases)

        if ctx is not None:
            self.__context = ctx
        else:
            self.__context = Context()

        self._property_classes = dict()
        for b in bases:
            d = getattr(b, '_property_classes', None)
            if d:
                self._property_classes.update(d)

        for k, v in dct.items():
            if isinstance(v, PThunk):
                c = v(self, k)
                self._property_classes[k] = c
                setattr(self, k, mp(c, k))

        def getter(target):
            ak = '_owm_rdf_type_property'
            attr = getattr(target, ak, None)
            if attr is None:
                attr = target.attach_property(RDFTypeProperty, name=ak)
            return attr

        self.rdf_type_property = PropertyProperty(None, getter, cls_thunk=_get_rdf_type_property)
        for k, v in dct.items():
            if isinstance(v, Alias):
                setattr(self, k, getattr(self, v.target.result.linkName))
                self._property_classes[k] = v.target.result

        key_properties = dct.get('key_properties')
        if key_properties is not None:
            self.direct_key = False
            new_key_properties = []
            for kp in key_properties:
                if isinstance(kp, PThunk):
                    for k, p in self._property_classes.items():
                        if p is kp.result:
                            new_key_properties.append(k)
                            break
                    else:
                        raise Exception(f"The provided 'key_properties' entry, {kp},"
                                " does not appear to be a property")
                elif isinstance(kp, PropertyProperty):
                    for k, p in self._property_classes.items():
                        if p is kp._cls:
                            new_key_properties.append(k)
                            break
                    else:
                        raise Exception(f"The provided 'key_properties' entry, {kp},"
                                " does not appear to be a property for this class")
                elif isinstance(kp, six.string_types):
                    new_key_properties.append(kp)
                else:
                    raise Exception("The provided 'key_properties' entry does not appear"
                            " to be a property")
            self.key_properties = tuple(new_key_properties)

        key_property = dct.get('key_property')

        def _process_key_property(kp):
            if kp is None:
                return
            if isinstance(kp, PThunk):
                for k, p in self._property_classes.items():
                    if p is kp.result:
                        new_key_property = k
                        break
                else:  # no break
                    raise Exception(("The provided 'key_properties' entry, {},"
                            " does not appear to be a property").format(kp))
            elif isinstance(kp, PropertyProperty):
                for k, p in self._property_classes.items():
                    if p is kp._cls:
                        new_key_property = k
                        break
                else:
                    raise Exception(("The provided 'key_properties' entry, {},"
                            " does not appear to be a property for this class").format(
                                kp))
            elif isinstance(kp, six.string_types):
                new_key_property = kp
            else:
                raise Exception("The provided 'key_property' entry does not appear"
                        " to be a property")
            return new_key_property

        if key_property is not None:
            if self.key_properties is not None:
                raise Exception(f"key_properties is already defined as {self.key_properties}")
            self.key_property = _process_key_property(key_property)

        self.__query_form = None
        if not self.rdf_type_object_deferred:
            self.init_rdf_type_object()

    def contextualize_class_augment(self, context):
        '''
        For MappedClass, rdf_type and rdf_namespace have special behavior where they can
        be auto-generated based on the class name and base_namespace. We have to pass
        through these values to our "proxy" to avoid this behavior
        '''
        args = dict()
        if self.rdf_type_object is None:
            args['rdf_type_object_callback'] = lambda: self.rdf_type_object
        else:
            args['rdf_type_object'] = self.rdf_type_object

        res = super(ContextMappedClass, self).contextualize_class_augment(context, **args)
        res.__module__ = self.__module__
        return res

    def init_rdf_type_object(self):
        if self.rdf_type_object is None or self.rdf_type_object.identifier != self.rdf_type:
            if self.definition_context is None:
                raise Exception("The class {0} has no context for RDFSClass(ident={1})".format(
                    self, self.rdf_type))
            L.debug('Creating rdf_type_object for {} in {}'.format(self, self.definition_context))
            rdto = RDFSClass.contextualize(self.definition_context)(ident=self.rdf_type)
            for par in self.__bases__:
                prdto = getattr(par, 'rdf_type_object', None)
                if prdto is not None:
                    if rdto.identifier == prdto.identifier:
                        L.warning('Subclass %s of %s declared without a distinct rdf_type', self, par)
                        continue
                    rdto.rdfs_subclassof_property.set(prdto)
            self.augment_rdf_type_object(rdto)
            self.rdf_type_object = rdto

    def augment_rdf_type_object(self, rdf_type_object):
        '''
        Runs after initialization of the rdf_type_object
        '''
        pass

    def declare_class_registry_entry(self):
        self._check_is_good_class_registry()
        re = RegistryEntry.contextualize(self.context)()
        cd = self.declare_class_description()

        self.context.add_import(type(cd).definition_context)

        re.rdf_class(self.rdf_type)
        re.class_description(cd)
        self.context.add_import(self.definition_context)

    def declare_class_description(self):
        cd = PythonClassDescription.contextualize(self.context)()

        mo = PythonModule.contextualize(self.context)()
        mo.name(self.__module__)

        cd.module(mo)
        cd.name(self.__name__)

        return cd

    def _check_is_good_class_registry(self):
        module = IM.import_module(self.__module__)
        if hasattr(module, self.__name__):
            return

        ymc = getattr(module, '__yarom_mapped_classes__', None)
        if ymc and self in ymc:
            return

        L.warning('While saving the registry entry of {}, we found that its'
                  ' module, {}, does not have "{}" in its'
                  ' namespace'.format(self, self.__module__, self.__name__))

    @property
    def query(self):
        '''
        Creates a proxy that changes how some things behave for purposes of querying
        '''
        if self.__query_form is None:
            meta = type(self)
            self.__query_form = meta(self.__name__, (_QueryMixin, self),
                    dict(rdf_type=self.rdf_type,
                         rdf_type_object=self.rdf_type_object,
                         rdf_namespace=self.rdf_namespace,
                         schema_namespace=self.schema_namespace))
            self.__query_form.__module__ = self.__module__
        return self.__query_form

    def __call__(self, *args, no_type_decl=False, **kwargs):
        o = super(ContextMappedClass, self).__call__(*args, **kwargs)

        if no_type_decl:
            return o

        if isinstance(o, RDFSClass) and o.idl == R.RDFS.Class:
            o.rdf_type_property.set(o)
        elif isinstance(o, RDFProperty):
            RDFProperty.init_rdf_type_object()
            o.rdf_type_property.set(self.rdf_type_object)
        else:
            o.rdf_type_property.set(self.rdf_type_object)
        return o

    @property
    def context(self):
        return None

    @property
    def definition_context(self):
        """ Unlike self.context, definition_context isn't meant to be overriden """
        return self.__context

    def __setattr__(self, key, value):
        if isinstance(value, PThunk):
            c = value(self, key)
            self._property_classes[key] = c
            value = mp(c, key)
        super().__setattr__(key, value)


class _QueryMixin(object):
    '''
    Mixin for DataObject types to be used for executing queries. This is optional since queries can be executed with
    plain-old DataObjects. Use of the mixin is, however, recommended.

    Overrides the identifier generation logic. May do other things in the future.
    '''

    query_mode = True
    ''' An indicator that the object is in "query" mode allows for simple adaptations in subclasses.'''

    def defined_augment(self):
        return False


def _make_property(cls, property_type, *args, **kwargs):
    try:
        return cls._create_property(property_type=property_type, *args, **kwargs)
    except TypeError:
        return _partial_property(cls._create_property, property_type=property_type, *args, **kwargs)


class _partial_property(partial):
    pass


def contextualized_data_object(context, obj):
    res = contextualize_helper(context, obj)
    if obj is not res and hasattr(res, 'properties'):
        cprop = res.properties.contextualize(context)
        res.add_attr_override('properties', cprop)
        for p in cprop:
            res.add_attr_override(p.linkName, p)

        ctxd_owner_props = res.owner_properties.contextualize(context)
        res.add_attr_override('owner_properties', ctxd_owner_props)
    return res


class ContextualizableList(Contextualizable, list):
    '''
    A Contextualizable list
    '''
    def __init__(self, context):
        super(ContextualizableList, self).__init__()
        self._context = context

    def contextualize(self, context):
        res = type(self)(context=context)
        res += list(x.contextualize(context) for x in self)
        return res

    def decontextualize(self):
        res = type(self)(None)
        res += list(x.decontextualize() for x in self)
        return res


class ContextFilteringList(Contextualizable, set):
    def __init__(self, context):
        self._context = context

    def __iter__(self):
        for x in super(ContextFilteringList, self).__iter__():
            if self._context is None or x.context == self._context:
                yield x

    def contextualize(self, context):
        res = type(self)(context)
        res |= self
        return res

    def append(self, o):
        self.add(o)

    def decontextualize(self):
        return set(super(ContextFilteringList, self).__iter__())


class BaseDataObject(six.with_metaclass(ContextMappedClass,
                                        IdMixin,
                                        GraphObject,
                                        ContextualizableDataUserMixin)):

    """
    An object which can be mapped to an RDF graph

    Attributes
    -----------
    rdf_type : rdflib.term.URIRef
        The RDF type URI for objects of this type
    rdf_namespace : rdflib.namespace.Namespace
        The rdflib namespace (prefix for URIs) for instances of this class
    schema_namespace : rdflib.namespace.Namespace
        The rdflib namespace (prefix for URIs) for types that are part of this class'
        schema
    properties : list of owmeta_core.dataobject_property.Property or \
            owmeta_core.custom_dataobject_property.CustomProperty
        Properties belonging to this object
    owner_properties : list of owmeta_core.dataobject_property.Property or \
            owmeta_core.custom_dataobject_property.CustomProperty
        Properties belonging to parents of this object
    properties_are_init_args : bool
        If true, then properties defined in the class body can be passed as
        keyword arguments to __init__. For example::

            >>> class A(DataObject):
            ...     p = DatatypeProperty()

            >>> A(p=5)

        If the arguments are written explicitly into the __init__ method
        definition, then no special processing is done.
    """
    class_context = 'http://www.w3.org/2000/01/rdf-schema'
    rdf_type = R.RDFS['Resource']
    base_namespace = R.Namespace(BASE_SCHEMA_URL + "/")
    base_data_namespace = R.Namespace(BASE_DATA_URL + "/")
    hashfun = hashlib.md5

    _next_variable_int = 0

    properties_are_init_args = True

    key_properties = None

    key_property = None

    query_mode = False

    rdf_type_object_deferred = True

    def __new__(cls, *args, **kwargs):
        # This is defined so that the __init__ method gets a contextualized
        # instance, allowing for statements made in __init__ to be contextualized.
        ores = super(BaseDataObject, cls).__new__(cls)
        if cls.context is not None:
            ores.context = cls.context
            ores.add_contextualization(cls.context, ores)
            res = ores
        else:
            ores.context = None
            res = ores

        return res

    def __init__(self, **kwargs):
        ot = type(self)
        pc = ot._property_classes
        paia = ot.properties_are_init_args
        if paia:
            property_args = [(key, val) for key, val in ((k, kwargs.pop(k, None))
                                                         for k in pc)
                             if val is not None]
        self.__key = None
        super(BaseDataObject, self).__init__(**kwargs)
        self.properties = ContextualizableList(self.context)
        self.owner_properties = ContextFilteringList(self.context)

        self._variable = None

        for k, v in pc.items():
            if not v.lazy:
                self.attach_property(v, name=DATAOBJECT_PROPERTY_NAME_PREFIX + k)

        if paia:
            for k, v in property_args:
                getattr(self, k)(v)

    @property
    def rdf(self):
        '''
        Returns either the configured RDF graph or the `Context.rdf_graph` of its
        context
        '''
        if self.context is not None:
            return self.context.rdf_graph()
        else:
            return super(BaseDataObject, self).rdf

    @classmethod
    def next_variable(cls):
        cls._next_variable_int += 1
        return R.Variable('a' + cls.__name__ + '_' + str(cls._next_variable_int))

    @property
    def context(self):
        return self.__context

    @context.setter
    def context(self, value):
        self.__context = value

    def make_key_from_properties(self, names):
        '''
        Creates key from properties
        '''
        sdata = ''
        for n in names:
            prop = getattr(self, n)
            val = prop.defined_values[0]
            sdata += val.identifier.n3()
        return sdata

    def _key_defined(self):
        if self.__key is not None:
            return True
        elif self.query_mode:
            return False
        elif self.key_properties is not None:
            for k in self.key_properties:
                attr = getattr(self, k, None)
                if attr is None:
                    raise Exception('Key property "{}" is not available on object'.format(k))

                if not attr.has_defined_value():
                    return False
            return True
        elif self.key_property is not None:
            attr = getattr(self, self.key_property, None)
            if attr is None:
                raise Exception('Key property "{}" is not available on object'.format(
                    self.key_property))
            if not attr.has_defined_value():
                return False
            return True
        else:
            return False

    @property
    def key(self):
        if not self._key_defined():
            return None
        if self.__key is not None:
            return self.__key
        elif self.key_properties is not None:
            return self.make_key_from_properties(self.key_properties)
        elif self.key_property is not None:
            prop = getattr(self, self.key_property)
            val = prop.defined_values[0]
            if self.direct_key:
                return val.value
            else:
                return val
        else:
            return IdentifierMissingException()

    @key.setter
    def key(self, value):
        self.__key = value

    def __repr__(self):
        return '{}(ident={})'.format(self.__class__.__name__, repr(self.idl))

    def id_is_variable(self):
        """ Is the identifier a variable? """
        return not self.defined

    def triples(self, *args, **kwargs):
        return ComponentTripler(self, **kwargs)()

    def __str__(self):
        k = self.idl
        if self.namespace_manager is not None:
            k = self.namespace_manager.normalizeUri(k)
        return '{}({})'.format(self.__class__.__name__, k)

    def __setattr__(self, name, val):
        if isinstance(val, _partial_property):
            val(owner=self, linkName=name)
        else:
            super(BaseDataObject, self).__setattr__(name, val)

    def count(self):
        return len(GraphObjectQuerier(self, self.rdf, hop_scorer=goq_hop_scorer)())

    def load_terms(self, graph=None):
        '''
        Loads URIs by matching between the object graph and the RDF graph

        Parameters
        ----------
        graph : rdflib.graph.ConjunctiveGraph
            the RDF graph to load from
        '''
        return load_terms(self.rdf if graph is None else graph,
                          self,
                          type(self).rdf_type)

    def load(self, graph=None):
        '''
        Loads `DataObjects <.DataObject>` by matching between the object graph and the RDF graph

        Parameters
        ----------
        graph : rdflib.graph.ConjunctiveGraph
            the RDF graph to load from
        '''
        return load(self.rdf if graph is None else graph,
                    self,
                    type(self).rdf_type,
                    self.context,
                    _Resolver.get_instance())

    @property
    def expr(self):
        '''
        Create a query expression rooted at this object
        '''
        return DataObjectExpr(self)

    def variable(self):
        if self._variable is None:
            self._variable = self.next_variable()
        return self._variable

    __eq__ = object.__eq__
    '''
    `DataObject` comparison by identity by default.
    '''

    __hash__ = object.__hash__
    '''
    `DataObject` comparison by identity by default.
    '''

    def get_owners(self, property_class_name):
        """ Return a generator of owners along a property pointing to this object """
        for x in self.owner_properties:
            if str(x.__class__.__name__) == str(property_class_name):
                yield x.owner

    @classmethod
    def DatatypeProperty(cls, *args, **kwargs):
        """
        Attach a, possibly new, property to this class that has a simple type
        (string, number, etc) for its values

        Parameters
        ----------
        linkName : string
            The name of this property.
        owner : owmeta_core.dataobject.BaseDataObject
            The owner of this property.
        """
        return _make_property(cls, 'DatatypeProperty', *args, **kwargs)

    @classmethod
    def ObjectProperty(cls, *args, **kwargs):
        """
        Attach a, possibly new, property to this class that has a `BaseDataObject` for its
        values

        Parameters
        ----------
        linkName : string
            The name of this property.
        owner : owmeta_core.dataobject.BaseDataObject
            The owner of this property.
        value_type : type
            The type of BaseDataObject for values of this property
        """
        return _make_property(cls, 'ObjectProperty', *args, **kwargs)

    @classmethod
    def UnionProperty(cls, *args, **kwargs):
        """ Attach a, possibly new, property to this class that has a simple
        type (string,number,etc) or `BaseDataObject` for its values

        Parameters
        ----------
        linkName : string
            The name of this property.
        owner : owmeta_core.dataobject.BaseDataObject
            The owner of this property.
        """
        return _make_property(cls, 'UnionProperty', *args, **kwargs)

    @classmethod
    def _create_property_class(
            cls,
            linkName,
            property_type,
            value_type=None,
            value_rdf_type=None,
            multiple=False,
            link=None,
            lazy=True,
            inverse_of=None,
            mixins=(),
            **kwargs):

        owner_class = cls
        owner_class_name = owner_class.__name__
        property_class_name = str(owner_class_name + "_" + linkName)
        _PropertyTypes_key = (cls, linkName)

        if value_type is This:
            value_type = owner_class

        if value_type is None:
            value_type = BaseDataObject

        c = None
        if _PropertyTypes_key in PropertyTypes:
            c = PropertyTypes[_PropertyTypes_key]
        else:
            klass = None
            if property_type == "ObjectProperty":
                if value_type is not None and value_rdf_type is None:
                    value_rdf_type = value_type.rdf_type
                klass = SP.ObjectProperty
            else:
                value_rdf_type = None
                if property_type in ('DatatypeProperty', 'UnionProperty'):
                    klass = getattr(SP, property_type)

            if link is None:
                if owner_class.schema_namespace is None:
                    raise Exception("{}.schema_namespace is None".format(FCN(owner_class)))
                link = owner_class.schema_namespace[linkName]

            props = dict(linkName=linkName,
                         link=link,
                         value_rdf_type=value_rdf_type,
                         value_type=value_type,
                         owner_type=owner_class,
                         class_context=owner_class.definition_context,
                         rdf_object=RDFProperty.contextualize(owner_class.definition_context)(ident=link),
                         lazy=lazy,
                         multiple=multiple,
                         inverse_of=inverse_of,
                         **kwargs)

            if inverse_of is not None:
                invc = inverse_of[0]
                if invc is This:
                    invc = owner_class
                InverseProperty(owner_class, linkName, invc, inverse_of[1])

            c = type(property_class_name, mixins + (klass,), props)
            c.__module__ = owner_class.__module__
            PropertyTypes[_PropertyTypes_key] = c
        return c

    @classmethod
    def _create_property(cls, *args, **kwargs):
        owner = None
        if len(args) == 2:
            owner = args[1]
            args = (args[0],)
        else:
            owner = kwargs.get('owner', None)
            if owner is not None:
                del kwargs['owner']
        attr_name = kwargs.get('attrName')
        if owner is None:
            raise TypeError('No owner')
        return owner.attach_property(cls._create_property_class(*args, **kwargs), name=attr_name)

    def attach_property(self, c, name=None, ephemeral=False, **kwargs):
        '''
        Parameters
        ----------
        name : str
            The name to use for attaching to this dataobject
        ephemeral : bool
            If `True`, the property will not be set as an attribute on the object
        '''
        ctxd_pclass = c.contextualize_class(self.context)
        res = ctxd_pclass(owner=self,
                          conf=self.conf,
                          resolver=_Resolver.get_instance(),
                          **kwargs)

        # Even for "ephemeral", we need to add to `properties` so that queries and stuff
        # work.
        self.properties.append(res)

        if not ephemeral:
            if name is None:
                name = res.linkName

            setattr(self, name, res)

        return res

    def graph_pattern(self, shorten=False, show_namespaces=True, **kwargs):
        """ Get the graph pattern for this object.

        It should be as simple as converting the result of triples() into a BGP

        Parameters
        ----------
        shorten : bool
            Indicates whether to shorten the URLs with the namespace manager
            attached to the ``self``
        """

        nm = None
        if shorten:
            nm = self.namespace_manager
        return triples_to_bgp(self.triples(**kwargs), namespace_manager=nm,
                              show_namespaces=show_namespaces)

    def retract(self):
        """ Remove this object from the data store. """
        # Things to consider: because we do not have a closed-world assumption, a given
        # class cannot correctly delete all of the statements needed to "retract" all
        # statements about the object in the graph: properties that are not defined ahead
        # of time for the object may have been used to make statements about the object
        # and this class wouldn't know about them from the Python side. We do, however,
        # have some information about the properties themselves from the RDF graph and
        # from the class registry. Just like there should be only one Python class for a
        # given RDFS class, there should only be one Python class for each property
        # TODO: Actually finish this
        # TODO: Fix this up with contexts etc.
        for x in self.load():
            self.rdf.remove((x.identifier, None, None))

    def save(self):
        """ Write in-memory data to the database.
        Derived classes should call this to update the store.
        """
        self.add_statements(self.triples())

    @classmethod
    def object_from_id(cls, identifier_or_rdf_type, rdf_type=None):
        if not isinstance(identifier_or_rdf_type, URIRef):
            identifier_or_rdf_type = URIRef(identifier_or_rdf_type)

        context = DEF_CTX
        if cls.context is not None:
            context = cls.context

        if rdf_type is None:
            return oid(identifier_or_rdf_type, context=context)
        else:
            rdf_type = URIRef(rdf_type)
            return oid(identifier_or_rdf_type, rdf_type, context=context)

    def decontextualize(self):
        if self.context is None:
            return self
        res = decontextualize_helper(self)
        if self is not res:
            cprop = res.properties.decontextualize()
            res.add_attr_override('properties', cprop)
            for p in cprop:
                res.add_attr_override(p.linkName, p)
        return res

    def contextualize_augment(self, context):
        if context is not None:
            return contextualized_data_object(context, self)
        else:
            return self


class DataObjectExpr(object):
    def __init__(self, dataobject):
        self.dataobject = dataobject
        self.created_sub_expressions = dict()
        self.terms = None
        self.rdf = self.dataobject.rdf
        self.combos = []

    def terms_provider(self):
        return list(self.dataobject.load_terms())

    def to_terms(self):
        if self.terms is None:
            self.terms = self.terms_provider()
        return self.terms

    def to_objects(self):
        return list(SP.ExprResultObj(self, t) for t in self.to_terms())

    @property
    def rdf_type(self):
        '''
        Short-hand for `rdf_type_property`
        '''
        return self.rdf_type_property

    def __repr__(self):
        return f'{FCN(type(self))}({repr(self.dataobject)})'

    def property(self, property_class):
        link = property_class.link

        if ('link', link) in self.created_sub_expressions:
            return self.created_sub_expressions[('link', link)]

        triples_choices = self.rdf.triples_choices

        def terms_provider():
            terms = list(self.terms_provider())
            for c in triples_choices(
                    (terms, link, None)):
                yield c[2]

        def triples_provider():
            terms = list(self.terms_provider())
            for c in triples_choices(
                    (terms, link, None)):
                yield c

        res = SP.PropertyExpr([property_class],
                terms_provider=terms_provider,
                triples_provider=triples_provider,
                origin=self)
        self.created_sub_expressions[('link', property_class.link)] = res
        return res

    def __getattr__(self, attr):
        if ('attr', attr) in self.created_sub_expressions:
            return self.created_sub_expressions[('attr', attr)]

        sub_prop = getattr(self.dataobject, attr)

        if self.dataobject.defined:
            res = SP.PropertyExpr([sub_prop])
        else:
            link = sub_prop.link
            triples_choices = self.rdf.triples_choices

            def terms_provider():
                terms = list(self.terms_provider())
                for c in triples_choices(
                        (terms, link, None)):
                    yield c[2]

            def triples_provider():
                terms = list(self.terms_provider())
                for c in triples_choices(
                        (terms, link, None)):
                    yield c
            res = SP.PropertyExpr([sub_prop], terms_provider=terms_provider,
                    triples_provider=triples_provider,
                    origin=self)

        self.created_sub_expressions[('attr', attr)] = res
        return res


class _Resolver(RDFTypeResolver):
    instance = None

    @classmethod
    def get_instance(cls):
        if cls.instance is None:
            cls.instance = cls(
                BaseDataObject.rdf_type,
                get_most_specific_rdf_type,
                oid,
                deserialize_rdflib_term)
        return cls.instance


class RDFTypeProperty(SP.ObjectProperty):
    ''' Corresponds to the rdf:type predidcate '''
    class_context = RDF_CONTEXT
    link = R.RDF['type']
    linkName = "rdf_type_property"
    value_rdf_type = R.RDFS['Class']
    owner_type = BaseDataObject
    multiple = True
    lazy = False
    rdf_object_deferred = True
    rdf_type_object_deferred = True


class RDFSClass(BaseDataObject):
    ''' The GraphObject corresponding to rdfs:Class '''

    # XXX: This class may be changed from a singleton later to facilitate
    #      dumping and reloading the object graph
    rdf_type = R.RDFS['Class']
    class_context = ClassContext('http://www.w3.org/2000/01/rdf-schema')
    base_namespace = R.Namespace('http://www.w3.org/2000/01/rdf-schema#')
    rdf_type_object_deferred = True


class RDFSSubClassOfProperty(SP.ObjectProperty):
    ''' Corresponds to the rdfs:subClassOf predidcate '''
    class_context = 'http://www.w3.org/2000/01/rdf-schema'
    link = R.RDFS.subClassOf
    linkName = 'rdfs_subclassof_property'
    value_type = RDFSClass
    owner_type = RDFSClass
    multiple = True
    lazy = False
    rdf_object_deferred = True
    rdf_type_object_deferred = True


RDFSClass.rdfs_subclassof_property = CPThunk(RDFSSubClassOfProperty)


class RDFSSubPropertyOfProperty(SP.ObjectProperty):
    ''' Corresponds to the rdfs:subPropertyOf predidcate '''
    class_context = 'http://www.w3.org/2000/01/rdf-schema'
    link = R.RDFS['subPropertyOf']
    linkName = 'rdfs_subpropertyof'
    multiple = True
    lazy = True
    rdf_object_deferred = True
    rdf_type_object_deferred = True


class RDFSCommentProperty(SP.DatatypeProperty):
    ''' Corresponds to the rdfs:comment predicate '''
    class_context = 'http://www.w3.org/2000/01/rdf-schema'
    link = R.RDFS['comment']
    linkName = 'rdfs_comment'
    owner_type = BaseDataObject
    multiple = True
    lazy = True
    rdf_object_deferred = True
    rdf_type_object_deferred = True


class RDFSLabelProperty(SP.DatatypeProperty):
    ''' Corresponds to the rdfs:label predicate '''
    class_context = 'http://www.w3.org/2000/01/rdf-schema'
    link = R.RDFS['label']
    linkName = 'rdfs_label'
    owner_type = BaseDataObject
    multiple = True
    lazy = True
    rdf_object_deferred = True
    rdf_type_object_deferred = True


class RDFSMemberProperty(SP.UnionProperty):
    ''' Corresponds to the rdfs:member predicate '''
    class_context = 'http://www.w3.org/2000/01/rdf-schema'
    multiple = True
    owner_type = BaseDataObject
    link = R.RDFS.member
    link_name = 'rdfs_member'
    rdf_object_deferred = True
    rdf_type_object_deferred = True


BaseDataObject.rdfs_member = CPThunk(RDFSMemberProperty)
BaseDataObject.rdfs_label = CPThunk(RDFSLabelProperty)
BaseDataObject.rdfs_comment = CPThunk(RDFSCommentProperty)


class DataObject(BaseDataObject):
    '''
    An object that can be mapped to an RDF graph
    '''
    class_context = BASE_SCHEMA_URL
    rdf_type_object_deferred = True


class RDFProperty(BaseDataObject):
    """ The `DataObject` corresponding to rdf:Property """
    rdf_type = R.RDF.Property
    class_context = URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns')
    rdfs_subpropertyof = CPThunk(RDFSSubPropertyOfProperty)
    rdf_type_object_deferred = True


RDFSClass.init_rdf_type_object()
BaseDataObject.init_rdf_type_object()
DataObject.init_rdf_type_object()
RDFProperty.init_rdf_type_object()


RDFSSubPropertyOfProperty.value_type = RDFProperty
RDFSSubPropertyOfProperty.owner_type = RDFProperty


def disconnect():
    global PropertyTypes
    PropertyTypes.clear()


class ModuleAccessor(DataObject):
    '''
    Describes how to access a module.

    Module access is how a person or automated system brings the module to where it can be imported/included, possibly
    in a subsequent
    '''
    class_context = BASE_SCHEMA_URL


class Package(DataObject):
    ''' Describes an idealized software package identifiable by a name and version number '''
    class_context = BASE_SCHEMA_URL

    name = DatatypeProperty(__doc__='The standard name of the package')

    version = DatatypeProperty(__doc__='The version of the package')


class Module(DataObject):
    '''
    Represents a module of code

    Most modern programming languages organize code into importable modules of one kind or
    another. This is basically the nearest level above a *class* in the language.

    Modules are accessable by one or more `ModuleAccessor`
    '''
    class_context = BASE_SCHEMA_URL

    accessors = ObjectProperty(multiple=True, value_type=ModuleAccessor,
            __doc__='Ways to get the module')

    package = ObjectProperty(value_type=Package,
            __doc__='Package that provides the module')


class ClassDescription(DataObject):
    '''
    Describes a class in the programming language
    '''
    class_context = BASE_SCHEMA_URL

    module = ObjectProperty(value_type=Module,
            __doc__='The module the class belongs to')


class RegistryEntry(DataObject):
    '''
    A mapping from a class in the programming language to an RDF class.

    Objects of this type are utilized in the resolution of classes from the RDF graph
    '''
    class_context = BASE_SCHEMA_URL

    class_description = ObjectProperty(value_type=ClassDescription,
            __doc__='The description of the class')

    rdf_class = DatatypeProperty(__doc__='''
    The |RDF| type for the class

    We use rdf_type for the type of a `DataObject` (``RegistryEntry.rdf_type`` in this
    case), so we call this `rdf_class` to avoid the conflict
    ''')

    def defined_augment(self):
        return self.class_description.has_defined_value() and self.rdf_class.has_defined_value()

    def identifier_augment(self):
        return self.make_identifier(self.class_description.defined_values[0].identifier.n3() +
                                    self.rdf_class.defined_values[0].identifier.n3())


class PythonPackage(Package):
    ''' A Python package '''
    class_context = BASE_SCHEMA_URL
    key_properties = ('name', 'version')


class PythonModule(Module):
    '''
    A Python module
    '''
    class_context = BASE_SCHEMA_URL

    name = DatatypeProperty(__doc__='The full name of the module')

    key_property = 'name'
    direct_key = True

    def resolve_module(self):
        '''
        Load the module referenced by this object

        Returns
        -------
        types.ModuleType
            The module referenced by this object

        Raises
        ------
        ModuleResolutionFailed
            Raised if the class can't be resolved for whatever reason
        '''
        modname = self.name()
        if modname is None:
            raise ModuleResolutionFailed(f'No module name for {self}')
        try:
            return IM.import_module(modname)
        except ImportError:
            raise ModuleResolutionFailed(f'Could not import module named {modname}')


class PIPInstall(ModuleAccessor):
    '''
    Describes a `pip install` command line
    '''
    class_context = BASE_SCHEMA_URL

    name = DatatypeProperty()

    version = DatatypeProperty()


class PythonClassDescription(ClassDescription):
    '''
    Description for a Python class
    '''
    class_context = BASE_SCHEMA_URL

    name = DatatypeProperty(
            __doc__='Local name of the class (i.e., relative to the module name)')

    key_properties = (name, 'module')

    @classmethod
    def from_class(cls, other_cls):
        mod = PythonModule.contextualize_class(cls.context)()
        mod.name(other_cls.__module__)
        return cls(name=other_cls.__name__, module=mod)

    def resolve_class(self):
        '''
        Load the class described by this object

        Returns
        -------
        type
            The class described by this object

        Raises
        ------
        ClassResolutionFailed
            Raised if the class can't be resolved for whatever reason
        '''
        class_name = self.name()
        if not class_name:
            raise ClassResolutionFailed(f'No class name for {self}')

        moddo = self.module()
        if moddo is None:
            raise ClassResolutionFailed(f'No module reference for {self}')

        try:
            mod = moddo.resolve_module()
        except ModuleResolutionFailed as e:
            raise ClassResolutionFailed('Could not resolve the module') from e

        try:
            return getattr(mod, class_name)
        except AttributeError:
            raise ClassResolutionFailed(f'Class named {class_name} not found in module')


class ModuleResolutionFailed(Exception):
    '''
    Thrown when a `PythonModule` can't resolve its module
    '''


class ClassResolutionFailed(Exception):
    '''
    Thrown when a `PythonClassDescription` can't resolve its class
    '''


# Run all of the deferred RDF object initalizations

SP.Property.init_rdf_object()
SP.DatatypeProperty.init_rdf_object()
SP.ObjectProperty.init_rdf_object()
SP.UnionProperty.init_rdf_object()
RDFTypeProperty.init_rdf_object()
RDFSSubClassOfProperty.init_rdf_object()
RDFSSubPropertyOfProperty.init_rdf_object()
RDFSCommentProperty.init_rdf_object()
RDFSMemberProperty.init_rdf_object()
RDFSLabelProperty.init_rdf_object()
SP.Property.init_rdf_type_object()
SP.DatatypeProperty.init_rdf_type_object()
SP.ObjectProperty.init_rdf_type_object()
SP.UnionProperty.init_rdf_type_object()
