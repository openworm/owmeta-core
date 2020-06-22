from __future__ import print_function

import rdflib as R
import logging
from six import with_metaclass

from .utils import FCN
from .data import DataUser
from .context import Context
from .contextualize import (Contextualizable, ContextualizableClass,
                            contextualize_helper,
                            decontextualize_helper)
from .context_mapped_class_util import find_class_context, find_base_namespace
from .graph_object import (GraphObject,
                           GraphObjectQuerier,
                           ZeroOrMoreTQLayer)
from .inverse_property import InversePropertyMixin
from .property_mixins import (DatatypePropertyMixin,
                              UnionPropertyMixin)
from .property_value import PropertyValue
from .rdf_query_util import goq_hop_scorer, load
from .rdf_go_modifiers import SubClassModifier
from .statement import Statement
from .variable import Variable

import itertools
from lazy_object_proxy import Proxy

L = logging.getLogger(__name__)


class ContextMappedPropertyClass(ContextualizableClass):
    def __init__(self, name, bases, dct):
        super(ContextMappedPropertyClass, self).__init__(name, bases, dct)
        ctx = find_class_context(self, dct, bases)

        if ctx is not None:
            self.__definition_context = ctx
        else:
            self.__definition_context = None

        if not hasattr(self, 'base_namespace') or self.base_namespace is None:
            self.base_namespace = find_base_namespace(dct, bases)

    @property
    def definition_context(self):
        return self.__definition_context

    def after_mapper_module_load(self, mapper):
        '''
        Called after the module has been loaded. See :class:`owmeta_core.mapper.Mapper`
        '''
        self.init_python_class_registry_entries()

    def init_python_class_registry_entries(self):
        #self._check_is_good_class_registry()
        from owmeta_core.dataobject import (RegistryEntry, PythonClassDescription,
                                       PythonModule)
        re = RegistryEntry.contextualize(self.definition_context)()
        cd = PythonClassDescription.contextualize(self.definition_context)()

        mo = PythonModule.contextualize(self.definition_context)()
        mo.name(self.__module__)

        cd.module(mo)
        cd.name(self.__name__)

        re.rdf_class(self.rdf_type)
        re.class_description(cd)

    def __lt__(self, other):
        res = False
        if issubclass(other, self) and not issubclass(self, other):
            res = True
        elif issubclass(self, other) == issubclass(other, self):
            res = self.__name__ < other.__name__
        return res


class ContextualizedPropertyValue(PropertyValue):

    @property
    def context(self):
        return None


class _ContextualizableLazyProxy(Proxy, Contextualizable):
    """ Contextualizes its factory for execution """
    def contextualize(self, context):
        assert isinstance(self.__factory__, Contextualizable)
        self.__factory__ = self.__factory__.contextualize(context)
        return self

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.__factory__))


class _StatementContextRDFObjectFactory(Contextualizable):
    __slots__ = ('context', 'statement')

    def __init__(self, statement):
        self.context = None
        self.statement = statement

    def contextualize(self, context):
        temp = _StatementContextRDFObjectFactory(self.statement)
        temp.context = context
        return temp

    def __call__(self):
        if self.context is None:
            raise ValueError("No context has been set for this proxy")
        return self.statement.context.contextualize(self.context).rdf_object

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.statement))


class Property(with_metaclass(ContextMappedPropertyClass, DataUser, Contextualizable)):
    multiple = False
    link = R.URIRef("property")
    linkName = "property"
    base_namespace = R.Namespace("http://openworm.org/entities/")

    def __init__(self, owner, **kwargs):
        super(Property, self).__init__(**kwargs)
        self._v = []
        self.owner = owner
        self._hdf = dict()
        self.filling = False
        self._expr = None

    @property
    def expr(self):
        if self._expr is None:
            self._expr = PropertyExpr([self])
        return self._expr

    def contextualize_augment(self, context):
        self._hdf[context] = None
        res = contextualize_helper(context, self)
        if res is not self:
            cowner = context(res.owner)
            res.add_attr_override('owner', cowner)
        return res

    def decontextualize(self):
        self._hdf[self.context] = None
        return decontextualize_helper(self)

    def has_value(self):
        for x in self._v:
            if x.context == self.context:
                return True
        return False

    def has_defined_value(self):
        hdf = self._hdf.get(self.context)
        if hdf is not None:
            return hdf
        for x in self._v:
            if x.context == self.context and x.object.defined:
                self._hdf[self.context] = True
                return True
        return False

    def set(self, v):
        if v is None:
            raise ValueError('It is not permitted to declare a property to have value the None')

        if not hasattr(v, 'idl'):
            v = ContextualizedPropertyValue(v)

        if not self.multiple:
            self.clear()

        stmt = self._insert_value(v)
        if self.context is not None:
            self.context.add_statement(stmt)
        return stmt

    def clear(self):
        """ Clears values set *in all contexts* """
        self._hdf = dict()
        for x in self._v:
            assert self in x.object.owner_properties
            x.object.owner_properties.remove(self)
            self._v.remove(x)

    @property
    def defined_values(self):
        return tuple(x.object for x in self._v
                     if x.object.defined and x.context == self.context)

    @property
    def values(self):
        return tuple(self._values_helper())

    def _values_helper(self):
        for x in self._v:
            if x.context == self.context:
                # XXX: decontextualzing default context here??
                if self.context is not None:
                    yield self.context(x.object)
                elif isinstance(x.object, Contextualizable):
                    yield x.object.decontextualize()
                else:
                    yield x.object

    @property
    def rdf(self):
        if self.context is not None:
            return self.context.rdf_graph()
        else:
            return super(Property, self).rdf

    @property
    def identifier(self):
        return self.link

    def fill(self):
        self.filling = True
        try:
            self.clear()
            for val in self.get():
                self.set(val)
                fill = getattr(val, 'fill', True)
                filling = getattr(val, 'filling', True)
                if fill and not filling:
                    fill()
        finally:
            self.filling = False

    def get(self):
        if self.rdf is None:
            return ()
        results = None
        owner = self.owner
        if owner.defined:
            results = set()
            ident = owner.identifier
            for s, p, o in self.rdf.triples((ident, self.link, None)):
                results.add(o)
        else:
            v = Variable("var" + str(id(self)))
            self._insert_value(v)

            def _zomifier(rdf_type):
                if rdf_type and getattr(self, 'value_rdf_type', None) == rdf_type:
                    return SubClassModifier(rdf_type)
            g = ZeroOrMoreTQLayer(_zomifier, self.rdf)
            results = GraphObjectQuerier(v, g, parallel=False,
                                         hop_scorer=goq_hop_scorer)()
            self._remove_value(v)
        return results

    get_terms = get
    '''
    Get the `~rdflib.term.Node` instances matching this property query
    '''

    @classmethod
    def get_multiple(cls, subjects, graph=None):
        '''
        Get the values for several objects

        Parameters
        ----------
        subjects : iterable of rdflib.term.URIRef
        '''
        if not isinstance(subjects, list):
            subjects = list(subjects)
        if graph is None and cls.context is not None:
            graph = cls.context.rdf_graph()
        if graph is None:
            raise ValueError(f'Either the "context" for {cls} ({cls.context})'
                    ' must have a value for "rdf_graph()" or "graph" argument'
                    ' must be provided')
        triples_choices = graph.triples_choices

        return {c[0]: c[2]
                for c in triples_choices(
                    (subjects, cls.link, None))}

    get_multiple_terms = get_multiple
    '''
    Get the `~rdflib.term.Node` instances matching the property queries across several
    objects
    '''

    def _insert_value(self, v):
        stmt = Statement(self.owner, self, v, self.context)
        self._hdf[self.context] = None
        self._v.append(stmt)
        v.owner_properties.append(self)
        return stmt

    def _remove_value(self, v):
        assert self in v.owner_properties
        self._hdf[self.context] = None
        v.owner_properties.remove(self)
        self._v.remove(Statement(self.owner, self, v, self.context))

    unset = _remove_value

    def __call__(self, *args, **kwargs):
        """ If arguments are given ``set`` method is called. Otherwise, the ``get``
        method is called. If the ``multiple`` member is set to ``True``, then a
        Python set containing the associated values is returned. Otherwise, a
        single bare value is returned.
        """
        if len(args) > 0 or len(kwargs) > 0:
            return self.set(*args, **kwargs)
        else:
            r = self.get(*args, **kwargs)
            if self.multiple:
                return set(r)
            else:
                for a in r:
                    return a
                return None

    def __repr__(self):
        fcn = FCN(type(self))
        return '{}(owner={})'.format(fcn, repr(self.owner))

    def one(self):
        return next(iter(self.get()), None)

    def onedef(self):
        for x in self._v:
            if x.object.defined and x.context == self.context:
                return x.object
        return None

    @classmethod
    def on_mapper_add_class(cls, mapper):
        cls.rdf_type = cls.base_namespace[cls.__name__]
        cls.rdf_namespace = R.Namespace(cls.rdf_type + "/")
        return cls

    @property
    def defined_statements(self):
        return tuple(x for x in self._v
                     if x.object.defined and x.subject.defined)

    @property
    def statements(self):
        return self.rdf.quads((self.owner.idl, self.link, None, None))


class PropertyExpr(object):
    '''
    A property expression
    '''
    def __init__(self, props, triples_provider=None, terms_provider=None, origin=None):
        self.props = props
        self.prop = props[0]
        if origin is None:
            origin = self.prop
        self.origin = origin
        self.rdf = self.origin.rdf
        self.terms = None
        if triples_provider is None:
            triples_provider = self._make_triples

        self.triples_provider = triples_provider

        if terms_provider is None:
            terms_provider = self._compute_terms

        self.terms_provider = terms_provider

        self.created_sub_expressions = dict()

    def __or__(self, other):
        if self is other:
            return self

        def terms_provider():
            return itertools.chain(self.terms_provider(), other.terms_provider())

        def triples_provider():
            return itertools.chain(self.triples_provider(), other.triples_provider())

        return type(self)(self.props + other.props,
                terms_provider=terms_provider,
                triples_provider=triples_provider,
                origin=self.origin)

    def __getattr__(self, attr):
        if attr in self.created_sub_expressions:
            return self.created_sub_expressions[attr]
        value_types = []
        for prop in self.props:
            try:
                value_types.append(getattr(prop, 'value_type'))
            except AttributeError:
                raise AttributeError(attr)

        sub_links = []
        sub_props = []
        for v in value_types:
            sub_prop = getattr(v, attr)
            sub_props.append(sub_prop)
            sub_links.append(sub_prop.link)

        res = None
        for prop, sub_prop, link in zip(self.props, sub_props, sub_links):
            triples_choices = self.rdf.triples_choices

            def m():
                terms = list(self.terms_provider())
                for c in triples_choices(
                        (terms, link, None)):
                    yield c[2]

            def n():
                terms = list(self.terms_provider())
                for c in triples_choices(
                        (terms, link, None)):
                    yield c

            new_expr = type(self)([sub_prop], terms_provider=m, triples_provider=n,
                    origin=self.origin)
            if res is None:
                res = new_expr
                continue
            res = res | new_expr
        self.created_sub_expressions[attr] = res
        return res

    def _make_triples(self):
        for prop in self.props:
            for term in prop.get_terms():
                yield (prop.owner.identifier, prop.link, term)

    def _compute_terms(self):
        return list(itertools.chain(*(prop.get_terms() for prop in self.props)))

    def to_dict(self):
        res = dict()
        for s, p, o in self.triples_provider():
            values = res.get(s)
            if isinstance(values, set):
                values.add(o)
            if values is not None and values != o:
                res[s] = set([values, o])
            else:
                res[s] = o
        return res

    __call__ = to_dict

    def to_terms(self):
        if self.terms is None:
            terms = self.terms_provider()
            if not isinstance(terms, list):
                terms = list(terms)
            self.terms = terms
        return self.terms

    def __iter__(self):
        return iter(self.to_terms())

    def __repr__(self):
        return '({}).expr'.format(
                ' | '.join(repr(p) for p in self.props))


class _ContextualizingPropertySetMixin(object):
    def set(self, v):
        if isinstance(v, _ContextualizableLazyProxy):
            v = v.contextualize(self.context)
        return super(_ContextualizingPropertySetMixin, self).set(v)


class OPResolver(object):

    def __init__(self, context):
        self._ctx = context

    def id2ob(self, ident, typ):
        from .rdf_query_util import oid
        return oid(ident, typ, self._ctx)

    @property
    def type_resolver(self):
        from .dataobject import _Resolver
        return _Resolver.get_instance().type_resolver

    @property
    def deserializer(self):
        from .dataobject import _Resolver
        return _Resolver.get_instance().deserializer

    @property
    def base_type(self):
        from .dataobject import _Resolver
        return _Resolver.get_instance().base_type


class PropertyCountMixin(object):
    def count(self):
        return sum(1 for _ in super(PropertyCountMixin, self).get())


class ObjectProperty(InversePropertyMixin,
                     _ContextualizingPropertySetMixin,
                     PropertyCountMixin,
                     Property):

    def __init__(self, resolver=None, *args, **kwargs):
        super(ObjectProperty, self).__init__(*args, **kwargs)

    def contextualize_augment(self, context):
        res = super(ObjectProperty, self).contextualize_augment(context)
        if self is not res:
            res.add_attr_override('resolver', OPResolver(context))
        return res

    def set(self, v):
        if not isinstance(v, GraphObject):
            raise Exception(
                "An ObjectProperty only accepts GraphObject instances. Got a " +
                str(type(v)) + " a.k.a. " +
                " or ".join(str(x) for x in type(v).__bases__))
        return super(ObjectProperty, self).set(v)

    def get(self):
        idents = super(ObjectProperty, self).get()
        r = load(self.rdf, idents=idents, context=self.context,
                 target_type=self.value_rdf_type)
        return itertools.chain(self.defined_values, r)

    @property
    def statements(self):
        return itertools.chain(self.defined_statements,
                               (Statement(self.owner,
                                          self,
                                          self.id2ob(x[2]),
                                          Context(ident=x[3]))
                                for x in super(ObjectProperty, self).statements))


class DatatypeProperty(DatatypePropertyMixin, PropertyCountMixin, Property):

    def get(self):
        r = super(DatatypeProperty, self).get()
        s = set()
        unhashables = []
        for x in self.defined_values:
            val = self.resolver.deserializer(x.idl)
            try:
                s.add(val)
            except TypeError as e:
                unhashables.append(val)
                L.info('Unhashable type: %s', e)
        return itertools.chain(r, s, unhashables)

    def onedef(self):
        x = super(DatatypeProperty, self).onedef()
        return self.resolver.deserializer(x.identifier) if x is not None else x

    @property
    def statements(self):
        return itertools.chain(self.defined_statements,
                               (Statement(self.owner,
                                          self,
                                          self.resolver.deserializer(x[2]),
                                          Context(ident=x[3]))
                                for x in super(DatatypeProperty, self).statements))


class UnionProperty(_ContextualizingPropertySetMixin,
                    InversePropertyMixin,
                    UnionPropertyMixin,
                    PropertyCountMixin,
                    Property):

    """ A Property that can handle either DataObjects or basic types """
    def get(self):
        r = super(UnionProperty, self).get()
        s = set()
        for x in self.defined_values:
            if isinstance(x, R.Literal):
                s.add(self.resolver.deserializer(x.idl))
        return itertools.chain(r, s)


def _property_to_string(self):
    try:
        s = str(self.linkName) + "=`" + \
            ";".join(str(s) for s in self.defined_values) + "'"
    except AttributeError:
        s = str(self.linkName) + '(no defined_values)'
    return s
