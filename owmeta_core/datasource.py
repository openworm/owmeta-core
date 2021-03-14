from __future__ import print_function
from __future__ import absolute_import

from collections import OrderedDict, defaultdict
import logging

from rdflib.term import URIRef
import six

from . import BASE_CONTEXT
from .utils import FCN
from .context import Context
from .dataobject import (DataObject, ObjectProperty, DatatypeProperty, UnionProperty, This,
                         CPThunk)
from .data_trans.common_data import DS_NS, DS_DATA_NS

L = logging.getLogger(__name__)

INFO_PROP_PREFIX = '_info_prop_'


class FormatUtil(object):
    @staticmethod
    def collect_values(attr, stored):
        if stored:
            attr_vals = list()
            for x in attr.get():
                if x not in attr_vals:
                    attr_vals.append(x)
        else:
            attr_vals = attr.defined_values
        return attr_vals


class Informational(object):
    '''
    Defines a property on a `.DataSource`

    Attributes
    ----------
    name : str
        The name for the property
    description : str
        A description of the property
    also : tuple of Informational or list of Informational
        Other properties which, if set, set the value for this property. If multiple such
        "also" properties are set when the owning `DataSource` instance is defined, then
        a `DuplicateAlsoException` will be raised.
    default_override : object
        An override for the default value, typically set by setting the value in a
        `.DataSource` class dictionary. Importantly, this overrides an "also" value which
        would normally take precedence.
    default_value : object
        Default value if no other value is set
    multiple : boolean
        If `True`, then the property can take on multiple values for the same subject
    cls : type
        The `~owmeta_core.dataobject_property.Property` corresponding to this property
    '''

    def __init__(self, name=None, display_name=None, description=None,
                 default_value=None, property_type='DatatypeProperty',
                 multiple=True, property_name=None, also=(), subproperty_of=None,
                 **property_args):
        '''
        Parameters
        ----------
        name : str, optional
            Name for the property. If not provided here, then the name generally gets set
            to the name to which this object is assigned
        display_name : str, optional
            Display name for the property. If not provided here, then the `name` will be
            used for the display name
        description : str, optional
            A description of the property
        default_value : object, optional
            Value to use
        property_type : 'DatatypeProperty', 'ObjectProperty', or 'UnionProperty'
            The type of `~owmeta_core.dataobject_property.Property` to create from this object.
            Default is 'DatatypeProperty'
        multiple : boolean, optional
            Whether this property can have multiple values for the same object. Default is
            `True`
        property_name : str, optional
            The name of the property to use for attributes. `name` will be used if a value
            is not provided here
        also : Informational, tuple of Informational, or list of Informational; optional
            Other properties which, if set, will give their value to this property as well
        subproperty_of : Informational
            Declares that given Informational's corresponding Property is a subproperty of
        **property_args
            Additional arguments which will be passed into the class dictionary when the
            `~owmeta_core.dataobject_property.Property` corresponding to this object is created.
        '''
        self.name = name
        self._property_name = property_name
        self._display_name = display_name
        self.default_value = default_value
        self.description = description
        self.property_type = property_type
        self.multiple = multiple
        if also and not isinstance(also, (list, tuple)):
            also = (also,)
        self.also = also
        self.property_args = property_args

        self.default_override = None

        self.cls = None
        self.subproperty_of = subproperty_of

    def __get__(self, obj, owner):
        if obj is None:
            return self
        else:
            return getattr(obj, INFO_PROP_PREFIX + self.name)

    @property
    def display_name(self):
        '''
        The display name for the property.
        '''
        return self._display_name if self._display_name is not None else self.name

    @display_name.setter
    def display_name(self, val):
        self._display_name = val

    @property
    def property_name(self):
        '''
        The name of the property to use for attributes
        '''
        return self._property_name if self._property_name is not None else self.name

    @property_name.setter
    def property_name(self, v):
        self._property_name = v

    def copy(self):
        '''
        Copy to a new `Informational`
        '''
        res = type(self)()
        for x in vars(self):
            setattr(res, x, getattr(self, x))
        return res

    def __repr__(self):
        return ("Informational(name='{}',"
                " display_name={},"
                " default_value={},"
                " description={})").format(self.name,
                                          repr(self.display_name),
                                          repr(self.default_value),
                                          repr(self.description))

    # NOTE: This guy has to come last to avoid conflict with the decorator
    @property
    def property(self):
        return getattr(self.cls, INFO_PROP_PREFIX + self.name).property


class DuplicateAlsoException(Exception):
    pass


class DataSourceType(type(DataObject)):
    """A type for DataSources

    Sets up the graph with things needed for MappedClasses
    """

    def __init__(self, name, bases, dct):
        self.__info_fields = []
        others = []
        newdct = dict()
        keys = dct.keys()
        phase = 0
        while keys and phase < 2:
            unhandled_keys = list()
            for z in keys:
                meta = dct[z]
                if isinstance(meta, Informational):
                    if meta.cls is not None:
                        L.debug("Already created a Property from %s for %s. Not creating another for %s",
                                meta, meta.cls, self)
                        prop_name = INFO_PROP_PREFIX + meta.name
                        meta_owner_property_property = None
                        meta_owner_property = None
                        try:
                            meta_owner_property_property = getattr(meta.cls, prop_name)
                        except AttributeError:
                            if phase == 0:
                                try:
                                    meta_owner_property = newdct[prop_name]
                                except KeyError:
                                    L.debug('Unable to handle Informational %s on %s -- probably a reference to an'
                                            ' Informational defined on this same DataSource. Will re-process.',
                                            meta, self)
                                    unhandled_keys.append(z)
                                    continue
                            else:
                                raise

                        if meta_owner_property:
                            newdct[INFO_PROP_PREFIX + z] = meta_owner_property
                        else:
                            newdct[INFO_PROP_PREFIX + z] = CPThunk(meta_owner_property_property.property)

                        meta_copy = meta.copy()
                        meta_copy.cls = self
                        meta_copy.name = z
                        self.__info_fields.append(meta_copy)
                        setattr(self, z, meta_copy)
                    else:
                        meta.cls = self
                        meta.name = z
                        self.__info_fields.append(meta)

                        # Make the owmeta_core property
                        #
                        # We set the name for the property to the inf.name since that's how we
                        # access the info on this object, but the inf.property_name is used for
                        # the linkName so that the property's URI is generated based on that name.
                        # This allows to set an attribute named inf.property_name on self while
                        # still having access to the property through inf.name.
                        ptype = None
                        if meta.property_type == 'DatatypeProperty':
                            ptype = DatatypeProperty
                        elif meta.property_type == 'ObjectProperty':
                            ptype = ObjectProperty
                        elif meta.property_type == 'UnionProperty':
                            ptype = UnionProperty
                        else:
                            raise ValueError(f'Unrecognized property type {meta.property_type}')

                        property_args = dict(**meta.property_args)
                        superproperty = meta.subproperty_of
                        if isinstance(superproperty, Informational):
                            superproperty_property = newdct.get(INFO_PROP_PREFIX + superproperty.name)
                            if not superproperty_property:
                                try:
                                    superproperty_property = superproperty.property
                                except AttributeError:
                                    raise ValueError(f'{superproperty} is missing a Property definition')
                            property_args['subproperty_of'] = superproperty_property
                        elif isinstance(superproperty, (list, tuple)):
                            sps = []
                            for sp in superproperty:
                                superproperty_property = newdct.get(INFO_PROP_PREFIX + sp.name)
                                if not superproperty_property:
                                    try:
                                        superproperty_property = sp.property
                                    except AttributeError:
                                        raise ValueError(f'{sp} is missing a Property definition')
                                sps.append(superproperty_property)
                            property_args['subproperty_of'] = sps
                        elif meta.subproperty_of:
                            property_args['subproperty_of'] = meta.subproperty_of

                        newdct[INFO_PROP_PREFIX + meta.name] = ptype(
                                linkName=meta.property_name,
                                multiple=meta.multiple,
                                **property_args)
                else:
                    others.append((z, dct[z]))
            keys = unhandled_keys
            phase += 1

        for x in bases:
            if isinstance(x, DataSourceType):
                self.__info_fields += [inf.copy() for inf in x.__info_fields]

        for k, v in others:
            for i in range(len(self.__info_fields)):
                if self.__info_fields[i].name == k:
                    # This is for setting default values from a super-class. We copy the
                    # Informational because the default is baked-in to the Informational
                    # instance, and we want it to apply only to the sub-class
                    self.__info_fields[i].default_override = v
                    setattr(self, k, self.__info_fields[i])
                    break
            else: # no 'break'
                newdct[k] = v
        if not getattr(self, '__doc__', None):
            self.__doc__ = self._docstr()
        super(DataSourceType, self).__init__(name, bases, newdct)

    def _docstr(self):
        s = ''
        for inf in self.__info_fields:
            s += '{} : :class:`~owmeta_core.dataobject.{}`'.format(inf.display_name, inf.property_type) + \
                    ('\n    Attribute: `{}`'.format(inf.name if inf.property_name is None else inf.property_name)) + \
                    (('\n\n    ' + inf.description) if inf.description else '') + \
                    ('\n\n    Default value: {}'.format(inf.default_value) if inf.default_value is not None else '') + \
                    '\n\n'
        return s

    @property
    def info_fields(self):
        return self.__info_fields


class Transformation(DataObject):
    """
    Record of the how a `DataSource` was produced and the sources of the transformation
    that produced it.  Unlike the 'source' field attached to DataSources, the Translation
    may distinguish different kinds of input source to a transformation.
    """

    class_context = BASE_CONTEXT

    transformer = ObjectProperty()

    def defined_augment(self):
        return self.transformer.has_defined_value() and self.transformer.onedef().defined

    def identifier_augment(self):
        return self.make_identifier(self.transformer.onedef().identifier.n3())


class Translation(Transformation):
    '''
    A transformation where, notionally, the general character of the input is preserved.

    In contrast to just a transformation, a translation wouldn't just pick out, say, one
    record within an input source containing several, but would have an output source with o
    '''
    translator = ObjectProperty(subproperty_of=Transformation.transformer)


class DataSource(six.with_metaclass(DataSourceType, DataObject)):
    '''
    A source for data that can get translated into owmeta_core objects.

    The value for any field can be passed to `~DataSource.__init__` by name. Additionally,
    if the sub-class definition of a DataSource assigns a value for that field like::

        class A(DataSource):
            some_field = 3

    that value will be used over the default value for the field, but not over
    any value provided to `~DataSource.__init__`.
    '''

    class_context = BASE_CONTEXT

    source = Informational(display_name='Input source',
                           description='The data source that was translated into this one',
                           property_type='ObjectProperty',
                           value_type=This)

    transformation = Informational(display_name='Transformation',
                                   description='Information about the transformation process that created this object',
                                   property_type='ObjectProperty',
                                   value_type=Transformation,
                                   cascade_retract=True)

    translation = Informational(display_name='Translation',
                                description='Information about the translation process that created this object',
                                property_type='ObjectProperty',
                                subproperty_of=transformation,
                                value_type=Translation,
                                cascade_retract=True)

    description = Informational(display_name='Description',
                                description='Free-text describing the data source')

    base_namespace = DS_NS
    base_data_namespace = DS_DATA_NS

    def __init__(self, **kwargs):
        # There's a similar behavior in vanilla DataObject, but that doesn't have default
        # defaults and default-overrides. We don't pass the arguments up to DataObject so
        # the `properties_are_init_args` handling isn't used (whether
        # `properties_are_init_args` is True or False we get bad or incomplete behavior
        # when the property arguments are passed up)
        self.info_fields = OrderedDict((i.name, i) for i in self.__class__.info_fields)
        parent_kwargs = dict()
        new_kwargs = dict()
        for k, v in kwargs.items():
            if k not in self.info_fields:
                parent_kwargs[k] = v
            else:
                new_kwargs[k] = v
        super(DataSource, self).__init__(**parent_kwargs)
        vals = defaultdict(dict)
        for n, inf in self.info_fields.items():
            v = new_kwargs.get(n, None)
            if v is not None:
                vals[n]['i'] = v
            else:
                v = inf.default_value

            if inf.default_override is not None:
                vals[n]['e'] = inf.default_override

            vals[n]['d'] = inf.default_value

            for also in inf.also:
                if v is not None and vals[also.name].setdefault('a', v) != v:
                    raise DuplicateAlsoException('Only one also is allowed')

        for n, vl in vals.items():
            inf = self.info_fields[n]
            v = vl.get('i', vl.get('e', vl.get('a', vl['d'])))
            ctxd_prop = getattr(self, INFO_PROP_PREFIX + inf.name)
            if v is not None:
                ctxd_prop(v)

    def commit(self):
        '''
        Commit the data source *locally*

        This includes staging files such as they would be available for a translation. In general, a sub-class should
        implement :meth:`commit_augment` rather than this method, or at least call this method via super
        '''
        self.commit_augment()

    def commit_augment(self):
        pass

    def defined_augment(self):
        return self.transformation.has_defined_value() or self.translation.has_defined_value()

    def identifier_augment(self):
        '''
        It doesn't make much sense to have translation and transformation set, so we just
        take the first of them
        '''
        return (self.make_identifier(self.transformation.defined_values[0].identifier.n3())
                or self.make_identifier(self.translation.defined_values[0].identifier.n3()))

    def __str__(self):
        return self.format_str(False)

    def format_str(self, stored):
        try:
            sio = six.StringIO()
            print(self.__class__.__name__, end='', file=sio)
            if self.defined:
                ident = self.identifier
                if self.namespace_manager:
                    ident = self.namespace_manager.normalizeUri(ident)
                print(f'({ident})', file=sio)
            else:
                print(file=sio)
            for info in self.info_fields.values():
                attr = getattr(self, info.name)
                attr_vals = FormatUtil.collect_values(attr, stored)
                if attr_vals:
                    print('    ' + info.display_name, end=': ', file=sio)
                    for val in sorted(attr_vals):
                        val_line_sep = '\n      ' + ' ' * len(info.display_name)
                        if isinstance(val, (DataSource, GenericTranslation)):
                            valstr = val.format_str(stored)
                        elif isinstance(val, URIRef):
                            valstr = val.n3()
                        elif isinstance(val, six.string_types):
                            valstr = repr(val)
                        else:
                            valstr = str(val)
                        print(val_line_sep.join(valstr.split('\n')), end=' ', file=sio)
                    print(file=sio)
            return sio.getvalue()
        except AttributeError:
            res = super(DataSource, self).__str__()
            L.error('Failed while creating formatting string representation for %s', res)
            return res


class OneOrMore(object):
    """
    Wrapper for :class:`DataTransformer` input :class:`DataSource` types indicating that
    one or more of the wrapped type must be provided to the translator
    """
    def __init__(self, source_type):
        self.source_type = source_type

    def __repr__(self):
        return f"{FCN(type(self))}({self.source_type!r})"


class GenericTranslation(Translation):
    """
    A generic translation that just has sources in any order
    """

    class_context = BASE_CONTEXT

    source = ObjectProperty(multiple=True, value_rdf_type=DataSource.rdf_type)

    def defined_augment(self):
        return super(GenericTranslation, self).defined_augment() and \
                self.source.has_defined_value()

    def identifier_augment(self):
        data = super(GenericTranslation, self).identifier_augment().n3() + \
                "".join(sorted(x.identifier.n3() for x in self.source.defined_values))
        return self.make_identifier(data)

    def __str__(self):
        return self.format_str(False)

    def format_str(self, stored):
        sio = six.StringIO()
        print(f'{self.__class__.__name__}({self.idl})', file=sio)
        sources_field_name = 'Sources: '
        print(sources_field_name, end='', file=sio)

        attr = self.source
        attr_vals = FormatUtil.collect_values(attr, stored)

        if attr_vals:
            val_line_sep = '\n' + len(sources_field_name) * ' '
            print(val_line_sep.join(val_line_sep.join(val.format_str(stored).split('\n'))
                                    for val in sorted(attr_vals)), file=sio)

        if stored:
            translator = self.translator.one()
        else:
            translator = self.translator.onedef()
        if translator is not None:
            field = "Translator: "
            s = ('\n' + len(field) * ' ').join(str(translator).split('\n'))
            print(field + s, file=sio)
        return sio.getvalue()


class DataObjectContextDataSource(DataSource):

    class_context = BASE_CONTEXT

    def __init__(self, context, **kwargs):
        super(DataObjectContextDataSource, self).__init__(**kwargs)
        if context is not None:
            self.context = context
        else:
            self.context = Context()


def format_types(typ):
    if isinstance(typ, OneOrMore):
        return ':class:`{}` (:class:`~{}`)'.format(FCN(OneOrMore), FCN(typ.source_type))
    elif isinstance(typ, type):
        return ':class:`~{}`'.format(FCN(typ))
    else:
        return ', '.join(':class:`~{}`'.format(FCN(x)) for x in typ)


class DataTransformerType(type(DataObject)):
    def __init__(self, name, bases, dct):
        super(DataTransformerType, self).__init__(name, bases, dct)

        if not getattr(self, '__doc__', None):
            self.__doc__ = f'''Input type(s): {format_types(self.input_type)}\n
                               Output type(s): {format_types(self.output_type)}\n'''


class DataTransformer(six.with_metaclass(DataTransformerType, DataObject)):
    '''
    Transforms one or more `DataSources <DataSource>` to one or more other `DataSources
    <DataSource>`

    Attributes
    ----------
    input_type : type or tuple of type
        Types of input to this transformer. Types should be sub-classes of `DataSource`
    output_type : type or tuple of type
        Types of output from this transformer. Types should be sub-classes of `DataSource`
    transformation_type : type
        Type of the `Transformation` record produced as a side-effect of transforming with
        this transformer
    output_key : str
        The "key" for outputs from this transformer. See `IdentifierMixin`
    '''

    class_context = BASE_CONTEXT

    input_type = DataSource
    output_type = DataSource
    transformation_type = Transformation

    def __call__(self, *args, **kwargs):
        self.output_key = kwargs.pop('output_key', None)
        self.output_identifier = kwargs.pop('output_identifier', None)
        try:
            return self.transform(*args, **kwargs)
        finally:
            self.output_key = None
            self.output_identifier = None

    def __str__(self):
        s = '''Input type(s): {}
               Output type(s): {}'''.format(self.input_type,
                                            self.output_type)
        return f'{FCN(type(self))}({self.idl})' + ': \n    ' + ('\n    '.join(x.strip() for x in s.split('\n')))

    def defined_augment(self):
        return True

    def identifier_augment(self):
        return self.make_identifier(type(self).rdf_type)

    def transform(self, *args, **kwargs):
        '''
        Notionally, this method takes a data source, which is transformed into some other
        data source. There doesn't necessarily need to be an input data source.

        Parameters
        ----------
        *args
            Input data sources
        **kwargs
            Named input data sources

        Returns
        -------
        the output data source
        '''
        raise NotImplementedError

    def make_transformation(self, sources=()):
        '''
        It's intended that implementations of `DataTransformer` will override this
        method to make custom `Transformations <Transformation>` according with how different
        arguments to `transform` are (or are not) distinguished.

        The actual properties of a `Transformation` subclass must be assigned within the
        `transform` method
        '''
        return self.transformation_type.contextualize(self.context)(transformer=self)

    def make_new_output(self, sources, *args, **kwargs):
        '''
        Make a new output `DataSource`. Typically called within `transform`. The t
        '''
        trans = self.make_transformation(sources)
        res = self.output_type.contextualize(self.context)(*args, transformation=trans,
                                                           ident=self.output_identifier,
                                                           key=self.output_key, **kwargs)
        for s in sources:
            res.source(s)

        return res


class BaseDataTranslator(DataTransformer):

    def translate(self, *args, **kwargs):
        '''
        Notionally, this method takes one or more data sources, and translates them into
        some other data source that captures essentially the same information, but in a
        different format. Additional sources can be passed in as well for auxiliary
        information which are not "translated" in their entirety into the output data
        source. Such auxiliarry data sources should be distinguished from the primary ones
        in the translation

        Parameters
        ----------
        *args
            Input data sources
        **kwargs
            Named input data sources

        Returns
        -------
        the output data source
        '''
        raise NotImplementedError

    def transform(self, *args, **kwargs):
        '''
        Just calls `translate` and returns its result.
        '''
        return self.translate(*args, **kwargs)

    def make_translation(self, sources=()):
        '''
        It's intended that implementations of `BaseDataTranslator` will override this
        method to make custom `Translations <Translation>` according with how different
        arguments to `translate` are (or are not) distinguished.

        The actual properties of a `Translation` subclass must be assigned within the
        `transform` method

        Parameters
        ----------
        sources : tuple
            The sources that go into the translation. Sub-classes may choose to pass these
            to their superclass' make_translation method or not.

        Returns
        -------
        a description of the translation

        '''
        return self.translation_type.contextualize(self.context)(transformer=self)

    def make_transformation(self, sources=()):
        '''
        Just calls `make_translation` and returns its result.
        '''
        return self.make_translation(sources)


class DataTranslator(BaseDataTranslator):
    """
    A specialization with the :class:`GenericTranslation` translation type that adds
    sources for the translation automatically when a new output is made
    """

    class_context = BASE_CONTEXT

    translation_type = GenericTranslation

    def make_translation(self, sources=()):
        res = super(DataTranslator, self).make_translation(sources)
        for s in sources:
            res.source(s)
        return res


class PersonDataTranslator(BaseDataTranslator):
    """
    A person who was responsible for carrying out the translation of a data source
    manually
    """

    class_context = BASE_CONTEXT

    person = ObjectProperty(multiple=True,
            __doc__='A person responsible for carrying out the translation.')

    # No translate impl is provided here since this is intended purely as a descriptive object
