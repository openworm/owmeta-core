from __future__ import print_function

import itertools
import logging
from importlib import import_module

import rdflib as R
from six import with_metaclass

from . import RDF_CONTEXT, BASE_SCHEMA_URL, BASE_DATA_URL
from .utils import FCN
from .data import DataUser
from .context import Context
from .contextualize import (Contextualizable, ContextualizableClass,
                            contextualize_helper,
                            decontextualize_helper)
from .context_mapped_class_util import find_class_context, find_base_namespace
from .graph_object import GraphObject, GraphObjectQuerier
from .inverse_property import InversePropertyMixin
from .mapped_class import MappedClass
from .property_mixins import (DatatypePropertyMixin,
                              UnionPropertyMixin)
from .property_value import PropertyValue
from .rdf_utils import deserialize_rdflib_term
from .rdf_query_modifiers import (rdfs_subpropertyof_zom,
                                  rdfs_subclassof_zom,
                                  ZeroOrMoreTQLayer,
                                  ContainerMembershipIsMemberTQLayer)
from .rdf_query_util import goq_hop_scorer, load_base
from .statement import Statement
from .variable import Variable

L = logging.getLogger(__name__)


class ContextMappedPropertyClass(MappedClass, ContextualizableClass):

    rdf_object_deferred = False
    rdf_type_object_deferred = False

    context_carries = ('rdf_object_deferred', 'rdf_type_object_deferred', 'link', 'linkName', 'rdf_type_class')

    def __init__(self, name, bases, dct):
        super(ContextMappedPropertyClass, self).__init__(name, bases, dct)
        ctx = find_class_context(self, dct, bases)

        self.linkName = dct.get('link_name', dct.get('linkName'))

        self.link = dct.get('link')

        if self.link is None and self.linkName is not None:
            self.link = self.base_namespace[self.linkName]

        if getattr(self, 'value_type', None) is not None and getattr(self, 'value_rdf_type', None) is None:
            self.value_rdf_type = self.value_type.rdf_type

        if 'definition_context' in dct:
            self.__definition_context = dct['definition_context']
        elif ctx is not None:
            self.__definition_context = ctx
        else:
            self.__definition_context = None

        if not hasattr(self, 'base_namespace') or self.base_namespace is None:
            self.base_namespace = find_base_namespace(dct, bases)

        self.rdf_type_class = dct.get('rdf_type_class')
        self.__rdf_object = dct.get('rdf_object')
        self.__rdf_object_callback = dct.get('rdf_object_callback')

        self.rdf_object_deferred = dct.get('rdf_object_deferred', False)
        self.rdf_type_object_deferred = dct.get('rdf_type_object_deferred', False)

        # We have to deferr initializing our rdf_object since initialization depends on
        # RDFSSubClassOfProperty being initialized
        if not self.rdf_object_deferred:
            self.init_rdf_object()

        if not self.rdf_type_object_deferred:
            self.init_rdf_type_object()

        if not getattr(self, 'unmapped', False) and not dct.get('unmapped'):
            module = import_module(self.__module__)
            if not hasattr(module, '__yarom_mapped_classes__'):
                module.__yarom_mapped_classes__ = [self]
            else:
                module.__yarom_mapped_classes__.append(self)

    @property
    def rdf_object(self):
        if self.__rdf_object_callback is not None:
            rdto = self.__rdf_object_callback()
            if rdto is not None:
                self.__rdf_object_callback = None
                self.__rdf_object = rdto
        return self.__rdf_object

    @rdf_object.setter
    def rdf_object(self, value):
        if value is not None:
            self.__rdf_object_callback = None
        self.__rdf_object = value

    def contextualize_class_augment(self, context):
        '''
        For MappedClass, rdf_type and rdf_namespace have special behavior where they can
        be auto-generated based on the class name and base_namespace. We have to pass
        through these values to our "proxy" to avoid this behavior
        '''
        args = dict()
        if self.rdf_object is None:
            args['rdf_object_callback'] = lambda: self.rdf_object
        else:
            args['rdf_object'] = self.rdf_object

        res = super(ContextMappedPropertyClass, self).contextualize_class_augment(
                context,
                **args)
        res.__module__ = self.__module__
        return res

    def init_rdf_object(self):
        # Properties created in a DataObject sub-class definition will have their
        # rdf_object created for them, obviating this procedure.
        if (getattr(self, 'link', None) is not None and
                (self.rdf_object is None or
                    self.rdf_object.identifier != self.link)):
            from .dataobject import RDFProperty
            if self.definition_context is None:
                L.info("The class {0} has no context for PropertyDataObject(ident={1})".format(
                    self, self.link))
                return
            L.debug('Creating rdf_object for {} in {}'.format(self, self.definition_context))
            rdto = RDFProperty.contextualize(self.definition_context)(ident=self.link)
            if hasattr(self, 'label'):
                rdto.rdfs_label(self.label)

            self.rdf_object = rdto

    def init_rdf_type_object(self):
        '''
        Sometimes, we actually use Property sub-classes *as* rdf:Property classes (e.g.,
        rdfs:ContainerMembershipProperty). The 'rdf_type' attribute has to be defined on
        this class if we're going to use it as a class.

        Here's where we can create the objects that describe the property classes.
        '''
        from .dataobject import RDFProperty

        rdf_type = getattr(self, 'rdf_type', None)
        if (rdf_type is not None and
                getattr(self, 'rdf_object', None) is None and
                getattr(self, 'rdf_type_class', None) is None):
            self.rdf_type_class = type(self.__name__,
                    (RDFProperty,),
                    dict(rdf_type=rdf_type,
                         class_context=self.definition_context))

    @property
    def definition_context(self):
        return self.__definition_context

    def declare_class_registry_entry(self):
        from owmeta_core.dataobject import RegistryEntry
        re = RegistryEntry.contextualize(self.context)()
        cd = self.declare_class_description()

        re.rdf_class(self.rdf_type)
        re.class_description(cd)

    def declare_class_description(self):
        from owmeta_core.dataobject import PythonClassDescription, PythonModule
        cd = PythonClassDescription.contextualize(self.context)()

        mo = PythonModule.contextualize(self.context)()
        mo.name(self.__module__)

        cd.module(mo)
        cd.name(self.__name__)
        return cd

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
    '''
    A property attached to a `~owmeta_core.dataobject.DataObject`.
    '''

    multiple = False
    '''
    If `True`, then the property will only maintain a single staged value at a time. No
    effort is made to check how many values are stored in the RDF graph.
    '''

    class_context = RDF_CONTEXT
    link = None
    linkName = None
    cascade_retract = False
    base_namespace = R.Namespace(BASE_SCHEMA_URL + '/')
    base_data_namespace = R.Namespace(BASE_DATA_URL + '/')

    lazy = True
    '''
    If `True`, then the property is not attached to an instance until the property is set
    or queried.
    '''

    rdf_object_deferred = True
    rdf_type_object_deferred = True

    def __init__(self, owner, **kwargs):
        super(Property, self).__init__(**kwargs)
        self._v = []
        self.owner = owner
        self._hdf = dict()
        self._expr = None

    @property
    def expr(self):
        '''
        An query expression from this property
        '''
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
        '''
        Returns `True` if there is a value set on this property in the currrent context
        '''
        for x in self._v:
            if x.context == self.context:
                return True
        return False

    def has_defined_value(self):
        '''
        Returns `True` if this property has a value in the current context which is either
        a `GraphObject` with `defined` set to `True` or a literal value
        '''
        hdf = self._hdf.get(self.context)
        if hdf is not None:
            return hdf
        for x in self._v:
            if x.context == self.context and x.object.defined:
                self._hdf[self.context] = True
                return True
        return False

    def set(self, v):
        '''
        Set the value for or add a value to this property
        '''
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
        '''
        The "defined" values set on this property in the current context
        '''
        return tuple(x.object for x in self._v
                     if x.object.defined and x.context == self.context)

    @property
    def values(self):
        '''
        Return all values set on this property in the current context
        '''
        return tuple(self._values_helper())

    def _values_helper(self):
        context = self.context
        for x in self._v:
            if x.context == context:
                # XXX: decontextualzing default context here??
                if context is not None:
                    yield context(x.object)
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
        '''
        Alias to `link`
        '''
        return self.link

    def get(self):
        if self.rdf is None:
            return ()
        results = None
        owner = self.owner

        g = ZeroOrMoreTQLayer(rdfs_subclassof_zom, self.rdf)
        g = ContainerMembershipIsMemberTQLayer(g)
        g = ZeroOrMoreTQLayer(rdfs_subpropertyof_zom(R.RDFS.member), g)
        if owner.defined:
            results = set()
            ident = owner.identifier
            for s, p, o in g.triples((ident, self.link, None)):
                results.add(o)
        else:
            v = Variable("var" + str(id(self)))
            self._insert_value(v)
            results = GraphObjectQuerier(v, g, hop_scorer=goq_hop_scorer)()
            self._remove_value(v)
        return results

    get_terms = get
    '''
    Get the `~rdflib.term.Node` instances matching this property query
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
    '''
    Remove a from this property
    '''

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
        return '{}(owner={})'.format(fcn, repr(getattr(self, 'owner', None)))

    def one(self):
        '''
        Query for a single value from this property.

        For a multi-valued property, the returned value is chosen arbitrarily. If there's
        no value returned from the query, then `None` is returned.

        '''
        return next(iter(self.get()), None)

    def onedef(self):
        '''
        Return a single defined value set on this property in the current context

        This does not execute a query, but returns a value which was set on this property.
        '''
        for x in self._v:
            if x.object.defined and x.context == self.context:
                return x.object
        return None

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
        self.dict = None
        self.combos = []

    def __or__(self, other):
        if self is other:
            return self

        def terms_provider():
            return itertools.chain(self.terms_provider(), other.terms_provider())

        def triples_provider():
            return itertools.chain(self.triples_provider(), other.triples_provider())

        res = type(self)(self.props + other.props,
                terms_provider=terms_provider,
                triples_provider=triples_provider,
                origin=self.origin)

        self.combos.append(res)
        other.combos.append(res)

        return res

    @property
    def rdf_type(self):
        '''
        Short-hand for `rdf_type_property`
        '''
        return self.rdf_type_property

    # Careful not to define any property descriptors below this definition
    def property(self, property_class):
        '''
        Create a sub-expression with the given property.

        Allows for creating expressions with properties that are not necessarily declared
        for the `value_type` of this expression's property
        '''
        if ('link', property_class.link) in self.created_sub_expressions:
            return self.created_sub_expressions[('link', property_class.link)]
        res = self._create_sub_expression(property_class)
        self.created_sub_expressions[('link', property_class.link)] = res
        return res

    def __getattr__(self, attr):
        if ('attr', attr) in self.created_sub_expressions:
            return self.created_sub_expressions[('attr', attr)]
        value_types = []
        for prop in self.props:
            try:
                value_types.append(getattr(prop, 'value_type'))
            except AttributeError:
                raise AttributeError(attr)

        sub_props = []
        for v in value_types:
            sub_prop = getattr(v, attr)
            sub_props.append(sub_prop)

        res = None
        for sub_prop in sub_props:
            new_expr = self._create_sub_expression(sub_prop)
            if res is None:
                res = new_expr
                continue
            res = res | new_expr
        self.created_sub_expressions[('attr', attr)] = res
        return res

    def _create_sub_expression(self, sub_prop):
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

        return type(self)([sub_prop],
                terms_provider=terms_provider,
                triples_provider=triples_provider,
                origin=self.origin)

    def to_dict(self, multiple=False):
        '''
        Return a `dict` mapping from identifiers for subjects of this expression's
        property to the objects for that property.

        Parameters
        ----------
        multiple : bool, optional
            If `False`, then only a single object is allowed for each subject in the
            results. An exception is raised if more than one object is found for a given
            subject.
        '''
        if self.dict is None:
            res = dict()
            triples = self.triples_provider()
            if not multiple:
                for s, p, o in triples:
                    current_value = res.get(s)
                    if current_value is not None and current_value != o:
                        raise PropertyExprError(f'More than one value for {p} in the'
                                f' results for {s}')
                    res[s] = o
            else:
                for s, p, o in triples:
                    values = res.get(s)
                    if values is None:
                        values = set([o])
                        res[s] = values
                    else:
                        values.add(o)
            self.dict = res
        return self.dict

    def to_objects(self):
        '''
        Returns a list of `ExprResultObj` that allow for retrieving results in a
        convenient attribute traversal
        '''
        return list(ExprResultObj(self, t) for t in self.to_terms())

    __call__ = to_dict

    def to_terms(self):
        '''
        Return a list of `rdflib.term.Node` terms produced by this expression.
        '''
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

    def _make_triples(self):
        # Note: This method only applies for the root property in the expression.
        # Sub-expressions have property *classes* which do not have a 'get_terms' method
        for prop in self.props:
            for term in prop.get_terms():
                yield (prop.owner.identifier, prop.link, term)

    def _compute_terms(self):
        # Note: This method only applies for the root property in the expression.
        # Sub-expressions have property *classes* which do not have a 'get_terms' method
        return list(itertools.chain(*(prop.get_terms() for prop in self.props)))


class PropertyExprError(Exception):
    pass


class ExprResultObj(object):
    '''
    Object returned by `PropertyExpr.to_objects`. Attributes for which
    `PropertyExpr.to_dict` has been called can be accessed on the object. For example we
    can print out the ``b`` properties of instances of a class ``A``::

        class B(DataObject):
            v = DatatypeProperty()

        class A(DataObject):
            b = ObjectProperty(value_type=B)

        a = A().a.expr
        a.b.v()
        for anA in a.to_objects():
            print(anA.identifier, anA.b)

    ``anA`` is an `ExprResultObj` in the example. The
    '''
    __slots__ = ('_expr', 'identifier')

    def __init__(self, expr, ident):
        self._expr = expr
        self.identifier = ident

    @property
    def rdf_type(self):
        '''
        Allias to rdf_type_property
        '''
        return self.rdf_type_property

    def property(self, property_class):
        '''
        Return the results object for this sub-expression

        Parameters
        ----------
        property_class : Property, Property sub-class, URIRef, or str
        '''
        if isinstance(property_class, str):
            link = R.URIRef(property_class)
        else:
            try:
                link = property_class.link
            except AttributeError:
                raise ValueError('Expected either an object with a `link` attribute or a'
                        ' rdflib.term.URIRef or a str')
        sub_expr = self._expr.created_sub_expressions.get(('link', link))
        if not sub_expr:
            for c in self._expr.combos:
                sub_expr = c.created_sub_expressions.get(('link', link))
                if sub_expr:
                    break

        if not sub_expr:
            raise KeyError(property_class)
        return self._give_value(sub_expr)

    def _give_value(self, sub_expr):
        sub_expr_dict = sub_expr.to_dict()
        val = sub_expr_dict.get(self.identifier)
        if val and (sub_expr.created_sub_expressions or
                any(c.created_sub_expressions for c in sub_expr.combos)):
            return type(self)(sub_expr, val)
        else:
            if isinstance(val, R.Literal):
                return deserialize_rdflib_term(val)
            return val

    def __getattr__(self, attr):
        sub_expr = self._expr.created_sub_expressions.get(('attr', attr))
        if not sub_expr:
            for c in self._expr.combos:
                sub_expr = c.created_sub_expressions.get(('attr', attr))
                if sub_expr:
                    break
        if not sub_expr:
            raise AttributeError(attr)
        return self._give_value(sub_expr)

    def __repr__(self):
        return f'{FCN(type(self))}({repr(self._expr)}, {repr(self.identifier)})'


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
                     PropertyCountMixin,
                     Property):

    rdf_object_deferred = True
    rdf_type_object_deferred = True

    def __init__(self, resolver, *args, **kwargs):
        super(ObjectProperty, self).__init__(*args, **kwargs)
        self.resolver = resolver

    def contextualize_augment(self, context):
        res = super(ObjectProperty, self).contextualize_augment(context)
        if context is not None:
            if self is not res:
                res.add_attr_override('resolver', context(self.resolver))
        return res

    def set(self, v):
        if not isinstance(v, GraphObject):
            raise Exception(
                f"ObjectProperty {self!r} only accepts GraphObject instances. Got a " +
                str(type(v)) + " a.k.a. " +
                " or ".join(str(x) for x in type(v).__bases__))
        return super(ObjectProperty, self).set(v)

    def get(self):
        idents = super(ObjectProperty, self).get()
        r = load_base(self.rdf,
                      idents,
                      self.value_rdf_type,
                      self.context,
                      self.resolver)
        return itertools.chain(self.defined_values, r)

    @property
    def statements(self):
        for x in self.get():
            yield Statement(self.owner,
                    self,
                    x,
                    self.context)


class DatatypeProperty(DatatypePropertyMixin, PropertyCountMixin, Property):

    rdf_object_deferred = True
    rdf_type_object_deferred = True

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
                                          ContextualizedPropertyValue(x[2]),
                                          Context(ident=x[3]))
                                for x in super(DatatypeProperty, self).statements))


class UnionProperty(InversePropertyMixin,
                    UnionPropertyMixin,
                    PropertyCountMixin,
                    Property):

    rdf_object_deferred = True
    rdf_type_object_deferred = True

    """ A Property that can handle either DataObjects or basic types """
    def get(self):
        r = super(UnionProperty, self).get()
        s = set()
        for x in self.defined_values:
            if isinstance(x, PropertyValue):
                s.add(self.resolver.deserializer(x.idl))
            else:
                s.add(x)
        return itertools.chain(r, s)

    def onedef(self):
        x = super(UnionProperty, self).onedef()
        if isinstance(x, PropertyValue):
            return self.resolver.deserializer(x.identifier) if x is not None else x
        return x

    @property
    def statements(self):
        for x in self.get():
            yield Statement(self.owner,
                    self,
                    x,
                    self.context)


def _property_to_string(self):
    try:
        s = str(self.linkName) + "=`" + \
            ";".join(str(s) for s in self.defined_values) + "'"
    except AttributeError:
        s = str(self.linkName) + '(no defined_values)'
    return s
