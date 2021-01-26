from collections.abc import Sequence
from contextlib import contextmanager
import copy
import logging
import re
from urllib.parse import unquote

from .context import ClassContext
from .dataobject import DataObject, DatatypeProperty, ObjectProperty, UnionProperty
from .datasource import DataSource, Informational
from .utils import ellipsize

L = logging.getLogger(__name__)


class SchemaException(Exception):
    '''
    Raised for an invalid input given to `TypeCreator`
    '''


class ValidationException(Exception):
    '''
    Raised for an invalid input given to `Creator`
    '''


class AssignmentValidationException(ValidationException):
    '''
    Raised when an attempt is made to assign an inappropriate value with `Creator`
    '''


class Creator:
    '''
    Creates objects based on a JSON schema augmented with type annotations as would be
    produced by :py:class:`TypeCreator`

    Currently, only annotations for JSON objects are supported. In the future, conversions
    for all types (arrays, numbers, ints, strings) may be supported.
    '''

    def __init__(self, schema):
        '''
        Takes a schema annotated with '_owm_type' entries indicating which types are
        expected at each position in the object and produces an instance of the root type
        described in the schema

        Parameters
        ----------
        schema : dict
            The annotated schema
        '''
        self.path_stack = []
        self._root_identifier = None
        self.schema = schema

    @contextmanager
    def _pushing(self, path_component):
        self.path_stack.append(path_component)
        try:
            yield
        finally:
            self.path_stack.pop()

    def gen_ident(self):
        if self._root_identifier:
            return self._root_identifier + '#' + '/'.join(str(x) for x in self.path_stack)

    def create(self, instance, ident=None):
        '''
        Creates an instance of the root OWM type given a deserialized instance of the type
        described in our JSON schema.

        A context can be passed in and it will be used to contextualize the OWM types

        Parameters
        ----------
        instance : dict
            The JSON object to create from
        context : owmeta_core.context.Context
            The context in which the object should be created

        Raises
        ------
        ValidationException
            Raised when there's an error with the given instance compared to the schema
        '''
        try:
            return self._create(instance, ident=ident)
        finally:
            del self.path_stack[:]
            self._root_identifier = None

    def fill_in(self, target, instance, ident=None):
        '''
        "Fill-in" an already existing target object with JSON matching a
        schema
        '''
        try:
            return self._create(instance, ident=ident, target=target)
        finally:
            del self.path_stack[:]
            self._root_identifier = None

    def _create(self, instance, schema=None, ident=None, target=None):
        if schema is None:
            schema = self.schema

        if ident is not None:
            self._root_identifier = ident

        if schema is False:
            raise AssignmentValidationException(schema, instance)

        if schema is True:
            return instance

        sRef = schema.get('$ref')

        if sRef:
            return self._create(instance, resolve_fragment(self.schema, sRef))

        sOneOf = schema.get('oneOf')
        if sOneOf:
            for opt in sOneOf:
                try:
                    return self._create(instance, opt)
                except AssignmentValidationException:
                    L.debug('oneOf option mismatch', exc_info=True)
            raise AssignmentValidationException(schema, instance)

        if instance is None:
            default = schema.get('default', None)
            # If the default is None, then it'll just fail below
            if default is not None:
                return self._create(default, schema)
            return None

        # TODO: Support allOf -- just added sufficient to process WCON schema for now
        # (2020/12/28)

        sType = schema.get('type')

        if sType is None:
            # At this point, we should have gotten all of the options other than a type,
            # so if we don't have a type, then we default to a "True" schema
            # interpretation
            return instance

        if isinstance(instance, str):
            if sType == 'string':
                return instance
            raise AssignmentValidationException(schema, instance)
        elif isinstance(instance, bool):
            # remember bool is a subtype of int, so boolean has to precede int
            if sType == 'boolean':
                return instance
            raise AssignmentValidationException(schema, instance)
        elif isinstance(instance, int):
            if sType in ('integer', 'number'):
                return instance
            raise AssignmentValidationException(schema, instance)
        elif isinstance(instance, float):
            if sType == 'number':
                return instance
            raise AssignmentValidationException(schema, instance)
        elif isinstance(instance, list):
            if sType == 'array':
                item_schema = schema.get('items')
                if item_schema:
                    converted_list = self.begin_sequence(schema)
                    for idx, elt in enumerate(instance):
                        with self._pushing(idx):
                            converted_list = self.add_to_sequence(
                                    schema, converted_list, idx, self._create(elt, item_schema))
                    return converted_list
                else:
                    # The default for items is to accept all, so we short-cut here...
                    # also means that there's OWM type conversion
                    return instance
            raise AssignmentValidationException(schema, instance)
        elif isinstance(instance, dict):
            if sType == 'object':
                owm_type = schema.get('_owm_type')
                if not owm_type:
                    # If an object isn't annotated, we treat as an error -- alternatives
                    # like returning None or just 'instance' could both be surprising and
                    # not annotating an object is most likely a mistake in a TypeCreator
                    # sub-class.
                    raise AssignmentValidationException(schema, instance)

                pt_args = dict()
                for k, v in instance.items():
                    props = schema.get('properties', {})

                    # If patprops doesn't have anything, then we pick it up with
                    # additionalProperties
                    patprops = schema.get('patternProperties', {})

                    # additionalProperties doesn't have any keys to check, so we
                    # can just pass true down to the next level
                    addprops = schema.get('additionalProperties', True)

                    if props:
                        sub_schema = props.get(k)
                        if sub_schema:
                            with self._pushing(k):
                                pt_args[k] = self._create(v, sub_schema)
                            continue

                    if patprops:
                        found = False
                        for p in patprops:
                            if re.match(p, k):
                                with self._pushing(k):
                                    pt_args[k] = self._create(v, patprops[p])
                                found = True
                                break
                        if found:
                            continue

                    if addprops:
                        with self._pushing(k):
                            pt_args[k] = self._create(v, addprops)
                        continue

                    raise AssignmentValidationException(schema, instance, k, v)

                if target is not None:
                    res = target
                else:
                    # res must be treated as a black-box since sub-classes have total freedom
                    # as far as what substitution they want to make
                    res = self.make_instance(owm_type)

                for k, v in pt_args.items():
                    self.assign(res, k, v)
                return res
            raise AssignmentValidationException(schema, instance)
        else:
            raise AssignmentValidationException(schema, instance)

    def begin_sequence(self, schema):
        return list()

    def add_to_sequence(self, schema, sequence, index, item):
        sequence.append(item)
        return sequence

    def assign(self, obj, name, value):
        '''
        Assign the given value to a property with the given name on the object

        Parameters
        ----------
        obj : object
            The object to receive the assignment
        name : str
            The name on the object to assign to
        value : object
            The value to assign
        '''
        raise NotImplementedError()

    def make_instance(self, owm_type):
        '''
        Make an instance of the given type

        Parameters
        ----------
        owm_type : type
            The type for which an instance should be made
        '''
        raise NotImplementedError()


class DataObjectCreator(Creator):
    def create(self, instance, context=None, ident=None):
        '''
        Parameters
        ----------
        instance : dict
            The JSON object to create from
        context : owmeta_core.context.Context, optional
            The context in which the object should be created
        ident : str, optional
            The base identifier for created objects. Identifiers for attached objects will
            be generated based on this identifier by default.
        '''
        self.context = context
        try:
            return super().create(instance, ident=ident)
        finally:
            self.context = None

    def assign(self, obj, key, val):
        '''
        Assigns values to properties on the created objects. If the `obj` does not already
        have a property for the given `key`, then it will be created. This is how
        ``additionalProperties`` and ``patternProperties`` are supported.
        '''
        if not hasattr(obj, key):
            typ = type(obj)
            if isinstance(val, (str, float, bool, int)) or \
                    isinstance(val, list) and val and \
                    isinstance(val[0], (str, float, bool, int)):
                typ.DatatypeProperty(key, owner=obj)
            elif isinstance(val, dict):
                L.warning("Received an object of unknown type: %s", ellipsize(str(val), 40))
                typ.DatatypeProperty(key, owner=obj)
            else:
                if val is not None:
                    value_type = type(val)
                else:
                    value_type = None
                typ.ObjectProperty(key, value_type=value_type, owner=obj)
        getattr(obj, key)(val)

    def make_instance(self, owm_type):
        if self.context:
            owm_type = self.context(owm_type)
        return owm_type(ident=self.gen_ident())

    def fill_in(self, target, instance, context=None, ident=None):
        if ident is None and target.defined:
            ident = target.identifier

        if context is None:
            context = target.context
        self.context = context
        try:
            super().fill_in(target, instance, ident)
        finally:
            self.context = None


class TypeCreator(object):
    '''
    Creates OWM types from a JSON schema and produces a copy of the schema annotated with
    the created types.
    '''

    def __init__(self, name, schema, definition_base_name=''):
        '''
        Parameters
        ----------
        name : str
            The name of the root class and the base-name for all classes derived from a
            schema's properties
        schema : dict
            A JSON schema as would be returned by :py:func:`json.load`
        definition_base_name : str
            The base-name for types defined in the schema's definitions. optional.
            By default, definitions just take the capitalized form of their key in the
            "definitions" block
        '''
        self.base_name = name
        self.definition_base_name = definition_base_name
        self.schema = schema

    @classmethod
    def retrieve_type(self, annotated_schema, pointer=''):
        '''
        Look up the type created for the object at the given JSON pointer location

        Parameters
        ----------
        annotated_schema : dict
            Annotated schema as returned from `annotate`
        pointer : str, optional
            JSON pointer to the schema/sub-schema

        Returns
        -------
        type
            The type at the given JSON pointer location

        Raises
        ------
        LookupError
            Raised when the pointer has no referent in the given document or there's type
            associated with the referent
        '''
        try:
            subschema = resolve_json_pointer(annotated_schema, pointer)
        except Exception:
            raise
        else:
            try:
                return subschema['_owm_type']
            except KeyError as e:
                raise LookupError(f'No type at {pointer}') from e

    def annotate(self):
        '''
        Returns the annotated JSON schema
        '''
        self._references = []
        return self._make_object(self.schema)

    def _handle_ref(self, path, v):
        if self._references is not None:
            self._references.append((path, v['$ref']))

    def extract_name(self, path):
        '''
        Generates a class name from the path to the sub-schema

        Parameters
        ----------
        path : tuple
            Path to the sub-schema
        '''
        s = self.base_name

        if len(path) > 0 and path[0] == 'definitions':
            s = self.definition_base_name

        for idx, p in enumerate(path):
            if idx % 2 == 1:
                s += self._camelify(p.capitalize())
        return s

    def _camelify(self, s):
        # XXX: Should make more effort to ensure a valid identifier
        res = re.sub('_([a-zA-Z])', lambda mo: mo.group(1).upper(), s)
        res = re.sub('-([a-zA-Z])', lambda mo: mo.group(1).upper(), res)
        return res

    def _make_object(self, schema, path=()):
        annotated_definition_schemas = self._process_definitions(schema, path)

        annotated_property_schemas = None
        properties = schema.get('properties', None)
        if properties is not None:
            with self._processing_properties(path):
                annotated_property_schemas = {}
                for k, v in properties.items():
                    if v.get('type') == 'object':
                        prop_annnotated_schema = self._make_object(v,
                                path=path + ('properties', k))
                    else:
                        prop_annnotated_schema = copy.deepcopy(v)

                    # TODO: Handle oneOf here -- this happens to not matter for schemas we
                    # care about, but we should make this work in general

                    if '$ref' in v:
                        self._handle_ref(path + ('properties', k), v)
                    annotated_property_schemas[k] = prop_annnotated_schema

                    self.proc_prop(path, k, v)

        typ = self.create_type(path, schema)

        annotated = copy.deepcopy(schema)

        if annotated_property_schemas is not None:
            annotated['properties'] = annotated_property_schemas

        if annotated_definition_schemas is not None:
            annotated['definitions'] = annotated_definition_schemas

        annotated['_owm_type'] = typ

        if path == ():
            for schema_path, reference in self._references:
                self._annotate_obj(annotated, schema_path,
                                   resolve_fragment(annotated, reference))

        return annotated

    def proc_prop(self, path, key, value):
        '''
        Process property named `key` with the given `value`.

        The `path` will not include the key but will be the path of the definition that
        contains the property. For example, in::

            {"$schema": "http://json-schema.org/schema",
             "title": "Example Schema",
             "type": "object",
             "properties": {"data": {"type": "object",
                                     "properties": {
                                        "data_data": {"type": "string"}
                                     }}}}

        `proc_prop` would be called as ``.proc_prop((), 'data', {'type': 'object', ...})``
        for ``data``, but for ``data_data``, it would be called like
        ``.proc_prop(('properties', 'data'), 'data_data', {'type': 'string'})``

        Parameters
        ----------
        path : tuple
            The path to the given property.
        key : str
            The name of the property
        value : dict
            the definition of the property
        '''
        raise NotImplementedError()

    def create_type(self, path, schema):
        '''
        Create the OWM type.

        At this point, the properties for the schema will already be created.

        Parameters
        ----------
        path : tuple
            The path to the type
        schema : dict
            The JSON schema that applies to this type
        '''
        raise NotImplementedError()

    def _process_definitions(self, schema, path, references=None):
        annotated_definition_schemas = None
        definitions = schema.get('definitions', None)
        if definitions:
            annotated_definition_schemas = {}
            for k, v in definitions.items():
                if v.get('type') == 'object':
                    defn_annnotated_schema = self._make_object(v,
                            path=path + ('definitions', k))
                elif '$ref' in v:
                    self._handle_ref(path, v, references)
                else:
                    defn_annnotated_schema = copy.deepcopy(v)
                annotated_definition_schemas[k] = defn_annnotated_schema

        return annotated_definition_schemas

    @classmethod
    def _annotate_obj(self, obj, path, repl):

        if '_owm_type' not in repl:
            return

        if not path:
            obj['_owm_type'] = repl['_owm_type']
            return

        subpart = obj.get(path[0])
        if subpart:
            self._annotate_obj(subpart, path[1:], repl)


class DataObjectTypeCreator(TypeCreator):
    '''
    Creates DataObject types from a JSON Schema

    Attributes
    ----------
    cdict : dict
        Map from paths in the schema to the dictionaries that will be passed into the
        class definition. The path is the same as passed into create_type
    module : str
        The module in which classes will be defined
    '''
    def __init__(self, *args, module, context=None, **kwargs):
        '''
        Parameters
        ----------
        module : str
            The module in which classes will be defined
        context : owmeta_core.context.Context or str
            The class context in which the various types will be declared
        '''
        super(DataObjectTypeCreator, self).__init__(*args, **kwargs)
        self.cdict = dict()
        if context and not isinstance(context, str):
            context = context.identifier

        self.module = module

        if context is not None:
            self._context = ClassContext(ident=context)
        else:
            self._context = None

    @contextmanager
    def _processing_properties(self, path):
        self.cdict[path] = {}
        yield

    def proc_prop(self, path, k, v):
        property_type_string = self.determine_property_type(path, k, v)
        property_type = _DO_PROPERTY_TYPES[property_type_string]
        self.cdict[path][k] = property_type()

    def determine_property_type(self, path, k, v):
        '''
        Determine the type of property created by `proc_prop`
        '''
        res = 'DatatypeProperty'
        if v.get('type') == 'object':
            res = 'ObjectProperty'
        else:
            oneOf = v.get('oneOf')
            if oneOf:
                # TODO: find out if all options are objects or not. If they are, then
                # ObjectProperty. If some are, then UnionProperty, otherwise default
                # to DataTypeProperty
                types = set()
                for schema in oneOf:
                    types.add(self.determine_property_type(path, k, schema))
                if len(types) > 1:
                    res = 'UnionProperty'
                else:
                    try:
                        res = types.pop()
                    except KeyError:
                        raise SchemaException('oneOf must be non-empty', path, k, v)
            else:
                ref = v.get('$ref')
                if ref:
                    res = self.determine_property_type(path, k, resolve_fragment(self.schema, ref))

        return res

    def create_type(self, path, schema):
        cdict = dict(self.cdict.get(path, dict()))
        bases = self.select_base_types(path, schema)
        if 'class_context' not in cdict:
            cdict['class_context'] = self._context

        if '__doc__' not in cdict:
            doc = (schema.get('title', '') + '\n\n' +
                   schema.get('description', '')).strip()
            cdict['__doc__'] = doc

        if 'unmapped' not in cdict:
            cdict['unmapped'] = True

        res = type(self.extract_name(path),
                bases,
                dict(**cdict))

        res.__module__ = self.module
        return res

    def select_base_types(self, path, schema):
        '''
        Returns the base types for `create_type`

        Parameters
        ----------
        path : tuple
            The path to the sub-schema
        schema : dict
            The sub-schema at the path location
        '''
        return (DataObject,)


class DataSourceTypeCreator(DataObjectTypeCreator):
    '''
    Creates DataSource types from a JSON Schema
    '''

    def proc_prop(self, path, k, v):
        if not path:
            property_type_string = self.determine_property_type(path, k, v)
            self.cdict[path][k] = Informational(k, display_name=v.get('title'),
                                     description=v.get('description'),
                                     property_type=property_type_string)
        else:
            super().proc_prop(path, k, v)

    def select_base_types(self, path, schema):
        '''
        Returns the base types for `create_type`

        Parameters
        ----------
        path : tuple
            The path to the sub-schema
        schema : dict
            The sub-schema at the path location
        '''
        if not path:
            return (DataSource,)
        return super().select_base_types(path, schema)


_DO_PROPERTY_TYPES = {'DatatypeProperty': DatatypeProperty,
                      'ObjectProperty': ObjectProperty,
                      'UnionProperty': UnionProperty}


# Copied and modified from jsonschema...
def resolve_fragment(document, fragment):
    """
    Resolve a ``fragment`` within the referenced ``document``.

    Parameters
    ----------
    document : object
        The referent document. Typically a `collections.abc.Mapping` (e.g., a dict) or
        `collections.abc.Sequence`, but if fragment is ``#``, then the document is
        returned unchanged.
    fragment : str
        a URI fragment to resolve within it

    Returns
    -------
    object
        The part of the document referred to
    """
    _, pointer = fragment.split('#', 1)

    return resolve_json_pointer(document, unquote(pointer))


# Copied and modified from jsonschema...
def resolve_json_pointer(document, pointer):
    """
    Resolve a ``fragment`` within the referenced ``document``.

    Parameters
    ----------
    document : object
        The referent document. Typically a `collections.abc.Mapping` (e.g., a dict) or
        `collections.abc.Sequence`, but if fragment is ``#``, then the document is
        returned unchanged.
    pointer : str
        a JSON pointer to resolve in the document

    Returns
    -------
    object
        The part of the document referred to
    """
    if pointer == '':
        return document
    pointer = pointer.lstrip("/")
    parts = pointer.split("/") if pointer else ['']

    for part in parts:
        part = _TILDE_RE.sub(_tilde_repl, part)

        if isinstance(document, Sequence):
            # Array indexes should be turned into integers. The "-" value isn't valid
            # since we're not going to find a schema that isn't in the list
            part = int(part)

        try:
            document = document[part]
        except (TypeError, LookupError) as e:
            raise LookupError(f"Unresolvable JSON pointer: {pointer!r}") from e

    return document


def _tilde_repl(md):
    try:
        return _TILDE_REPL_TABLE[md[1]]
    except Exception:
        raise ValueError(f'Unsupported tilde escape {md[1]}')


_TILDE_RE = re.compile(r'~(.?)')
_TILDE_REPL_TABLE = {'1': '/', '0': '~'}
