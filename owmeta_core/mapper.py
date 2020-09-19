from __future__ import print_function
import importlib as IM
import logging
import uuid

import rdflib as R

from .dataobject import (BaseDataObject, DataObject, RegistryEntry,
                         PythonClassDescription, PythonModule, ClassDescription)
from .utils import FCN
from .configure import Configurable


__all__ = ["Mapper",
           "UnmappedClassException"]

L = logging.getLogger(__name__)


class UnmappedClassException(Exception):
    pass


class ClassRedefinitionAttempt(Exception):
    '''
    Thrown when a `.Mapper.add_class` is called on a class when a class with the same name
    has already been added to the mapper
    '''
    def __init__(self, mapper, maybe_cls, cls):
        super(ClassRedefinitionAttempt, self).__init__(
                'Attempted to add class %s to %s when %s had already been added' % (
                    maybe_cls, mapper, cls))


class Mapper(Configurable):
    '''
    Keeps track of relationships between classes, between modules, and between classes and modules
    '''
    def __init__(self, base_namespace=None, imported=(), name=None,
            class_registry_context=None, **kwargs):
        super(Mapper, self).__init__(**kwargs)

        """ Maps full class names (i.e., including the module name) to classes """
        self._mapped_classes = dict()

        """ Maps RDF types to properties of the related class """
        self._rdf_type_table = dict()

        if base_namespace is None:
            base_namespace = R.Namespace("http://example.com#")
        elif not isinstance(base_namespace, R.Namespace):
            base_namespace = R.Namespace(base_namespace)

        """ Base namespace used if a mapped class doesn't define its own """
        self.base_namespace = base_namespace

        """ Modules that have already been loaded """
        self.modules = dict()

        self.imported_mappers = imported

        if name is None:
            name = hex(id(self))
        self.name = name
        self.__class_registry_context_id = class_registry_context
        self.__class_registry_context = None
        self._bootstrap_mappings()

    @property
    def class_registry_context(self):
        if self.__class_registry_context is None:
            from . import BASE_CONTEXT
            from .context import Context, CLASS_REGISTRY_CONTEXT_KEY
            crctx_id = (self.__class_registry_context_id or
                        self.conf.get(CLASS_REGISTRY_CONTEXT_KEY, None) or
                        uuid.uuid4().urn)

            # XXX: Probably should get the connection in here to contextualize this
            # context
            crctx = Context(crctx_id, conf=self.conf, mapper=self)
            crctx.add_import(BASE_CONTEXT)
            self.__class_registry_context = crctx
        return self.__class_registry_context

    def _bootstrap_mappings(self):
        from .dataobject import Module

        # Add classes needed for resolving other classes...
        # XXX: Smells off...probably don't want to have to do this.
        self.process_classes(BaseDataObject, DataObject, PythonClassDescription, Module,
                             ClassDescription, PythonModule, RegistryEntry)

    def add_class(self, cls):
        '''
        Add a class to the mapper

        Parameters
        ----------
        cls : type
            The class to add to the mapper

        Raises
        ------
        ClassRedefinitionAttempt
            Thrown when `add_class` is called on a class when a class with the same name
            has already been added to the mapper
        '''
        cname = FCN(cls)
        maybe_cls = self._lookup_class(cname)
        if maybe_cls is not None:
            if maybe_cls is cls:
                return False
            else:
                raise ClassRedefinitionAttempt(self, maybe_cls, cls)
        L.debug("Adding class %s@0x%x", cls, id(cls))

        self._mapped_classes[cname] = cls
        L.debug('parents %s', parents_str(cls))

        if hasattr(cls, 'on_mapper_add_class'):
            cls.on_mapper_add_class(self)

        # This part happens after the on_mapper_add_class has run since the
        # class has an opportunity to set its RDF type based on what we provide
        # in the Mapper.
        self._rdf_type_table[cls.rdf_type] = cls
        return True

    def load_module(self, module_name):
        """ Loads the module. """
        module = self.lookup_module(module_name)
        if not module:
            module = IM.import_module(module_name)
            return self.process_module(module_name, module)
        else:
            return module

    def process_module(self, module_name, module):
        self.modules[module_name] = module
        self._module_load_helper(module)
        return module

    def process_class(self, *classes):
        for c in classes:
            self.add_class(c)

    process_classes = process_class

    def lookup_module(self, module_name):
        m = self.modules.get(module_name, None)
        if m is None:
            for p in self.imported_mappers:
                m = p.lookup_module(module_name)
                if m:
                    break
        return m

    def _check_is_good_class_registry(self, cls):
        module = IM.import_module(cls.__module__)
        if hasattr(module, cls.__name__):
            return

        ymc = getattr(module, '__yarom_mapped_classes__', None)
        if ymc and cls in ymc:
            return

        L.warning('While saving the registry entry of {}, we found that its'
                  ' module, {}, does not have "{}" in its'
                  ' namespace'.format(cls, cls.__module__, cls.__name__))

    def save(self):
        self.declare_python_class_registry_entry(*self._rdf_type_table.values())
        self.class_registry_context.save()

    def declare_python_class_registry_entry(self, *classes):
        cr_ctx = self.class_registry_context
        for cls in classes:
            self._check_is_good_class_registry(cls)
            re = RegistryEntry.contextualize(cr_ctx)()
            cd = PythonClassDescription.contextualize(cr_ctx)()

            mo = PythonModule.contextualize(cr_ctx)()
            mo.name(cls.__module__)

            cd.module(mo)
            cd.name(cls.__name__)

            re.rdf_class(cls.rdf_type)
            re.class_description(cd)
            cr_ctx.add_import(cls.definition_context)

    def load_registry_entries(self):
        cr_ctx = self.class_registry_context.stored
        return cr_ctx(RegistryEntry)().load()

    def resolve_class(self, uri, context):
        '''
        Look up the Python class for the given URI recovered from the given `~.Context`

        Parameters
        ----------
        uri : rdflib.term.URIRef
            The URI to look up
        context : .Context
            The context the URI was found in. May affect which Python class is returned.
        '''

        # look up the class in the registryCache
        c = self._rdf_type_table.get(uri)
        if c is not None:
            return c
        # otherwise, attempt to load into the cache by
        # reading the RDF graph.

        cr_ctx = self.class_registry_context.stored
        c = self._resolve_class(uri, cr_ctx)
        if c:
            self.add_class(c)
        return c

    def _resolve_class(self, uri, cr_ctx):
        re = cr_ctx(RegistryEntry)()
        re.rdf_class(uri)
        cd = cr_ctx(PythonClassDescription)()
        re.class_description(cd)
        c = None

        for cd_l in cd.load():
            class_name = cd_l.name()
            moddo = cd_l.module()
            modname = moddo.name()
            try:
                mod = IM.import_module(modname)
            except ModuleNotFoundError:
                L.warn('Did not find module %s', modname)
                continue
            c = getattr(mod, class_name, None)
            if c is not None:
                break
            L.warning('Did not find class %s in %s', class_name, mod.__name__)

            ymc = getattr(mod, '__yarom_mapped_classes__', None)
            if not ymc:
                L.warning('No __yarom_mapped_classes__ in %s, so cannot look up %s',
                        mod.__name__, class_name)
                continue

            matching_classes = tuple(mc for mc in ymc
                                     if mc.__name__ == class_name)
            if not matching_classes:
                L.warning('Did not find class %s in %s.__yarom_mapped_classes__',
                        class_name, mod.__name__)
                continue

            c = matching_classes[0]
            if len(matching_classes) > 1:
                L.warning('More than one class has the same name in'
                        ' __yarom_mapped_classes__ for %s, so we are picking'
                        ' the first one as the resolved class among %s',
                        mod, matching_classes)
            break
        return c

    def _module_load_helper(self, module):
        # TODO: Make this class selector pluggable
        return self.handle_mapped_classes(getattr(module, '__yarom_mapped_classes__', ()))

    def handle_mapped_classes(self, classes):
        res = []
        for cls in classes:
            if isinstance(cls, type) and self.add_class(cls):
                res.append(cls)
        return res

    def lookup_class(self, cname):
        """ Gets the class corresponding to a fully-qualified class name """
        ret = self._lookup_class(cname)
        if ret is None:
            raise UnmappedClassException((cname,))
        return ret

    def _lookup_class(self, cname):
        c = self._mapped_classes.get(cname, None)
        if c is None:
            for p in self.imported_mappers:
                c = p._lookup_class(cname)
                if c:
                    break
        else:
            L.debug('%s.lookup_class("%s") %s@%s',
                    repr(self), cname, c, hex(id(c)))
        return c

    def mapped_classes(self):
        for p in self.imported_mappers:
            for c in p.mapped_classes():
                yield
        for c in self._mapped_classes.values():
            yield c

    def __str__(self):
        if self.name is not None:
            return f'{type(self).__name__}(name="{str(self.name)}")'
        else:
            return super(Mapper, self).__str__()


class _ClassOrderable(object):
    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, other):
        self.cls is other.cls

    def __gt__(self, other):
        res = False
        ocls = other.cls
        scls = self.cls
        if issubclass(ocls, scls) and not issubclass(scls, ocls):
            res = True
        elif issubclass(scls, ocls) == issubclass(ocls, scls):
            res = scls.__name__ > ocls.__name__
        return res

    def __lt__(self, other):
        res = False
        ocls = other.cls
        scls = self.cls
        if issubclass(scls, ocls) and not issubclass(ocls, scls):
            res = True
        elif issubclass(scls, ocls) == issubclass(ocls, scls):
            res = scls.__name__ < ocls.__name__
        return res


def parents_str(cls):
    return ", ".join(p.__name__ + '@' + hex(id(p)) for p in cls.mro())
