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
    def __init__(self, base_namespace=None, imported=(), name=None, **kwargs):
        super(Mapper, self).__init__(**kwargs)

        """ Maps class names to classes """
        self.MappedClasses = dict()

        """ Maps classes to decorated versions of the class """
        self.DecoratedMappedClasses = dict()

        """ Maps RDF types to properties of the related class """
        self.RDFTypeTable = dict()

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
        self.__class_registry_context = None
        self._bootstrap_mappings()

    @property
    def class_registry_context(self):
        if self.__class_registry_context is None:
            crctx = self.conf.get('mapper.class_registry_context', None)
            if not crctx:
                from . import BASE_CONTEXT
                from .context import Context, CLASS_REGISTRY_CONTEXT_KEY
                crctxid = self.conf.get(CLASS_REGISTRY_CONTEXT_KEY, None)
                if not crctxid:
                    crctxid = uuid.uuid4().urn
                crctx = Context(crctxid, conf=self.conf)
                crctx.add_import(BASE_CONTEXT)
            self.__class_registry_context = crctx
        return self.__class_registry_context

    def _bootstrap_mappings(self):
        from .dataobject import Module

        # Add classes needed for resolving other classes...
        # XXX: Smells off...probably don't want to have to do this.
        self.process_classes(BaseDataObject, DataObject, PythonClassDescription, Module,
                ClassDescription, PythonModule, RegistryEntry)

    def decorate_class(self, cls):
        '''
        Extension point for subclasses of Mapper to apply an operation to all mapped classes
        '''
        return cls

    def add_class(self, cls):
        cname = FCN(cls)
        maybe_cls = self._lookup_class(cname)
        if maybe_cls is not None:
            if maybe_cls is cls:
                return False
            else:
                raise ClassRedefinitionAttempt(self, maybe_cls, cls)
        L.debug("Adding class %s@0x%x", cls, id(cls))

        self.MappedClasses[cname] = cls
        self.DecoratedMappedClasses[cls] = self.decorate_class(cls)
        L.debug('parents %s', parents_str(cls))

        if hasattr(cls, 'on_mapper_add_class'):
            cls.on_mapper_add_class(self)

        # This part happens after the on_mapper_add_class has run since the
        # class has an opportunity to set its RDF type based on what we provide
        # in the Mapper.
        self.RDFTypeTable[cls.rdf_type] = cls
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
        for c in self._module_load_helper(module):
            try:
                if hasattr(c, 'after_mapper_module_load'):
                    c.after_mapper_module_load(self)
            except Exception:
                L.warning("Failed to process class %s", c)
                continue
        return module

    def process_class(self, *classes):
        for c in classes:
            self.add_class(c)
            if hasattr(c, 'after_mapper_module_load'):
                c.after_mapper_module_load(self)

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
        self.declare_python_class_registry_entry(*self.RDFTypeTable.values())
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

    def resolve_class(self, uri):
        # look up the class in the registryCache
        c = self.RDFTypeTable.get(uri)

        if c is None:
            # otherwise, attempt to load into the cache by
            # reading the RDF graph.
            cr_ctx = self.class_registry_context.stored
            re = cr_ctx(RegistryEntry)()
            re.rdf_class(uri)
            cd = cr_ctx(PythonClassDescription)()
            re.class_description(cd)

            for cd_l in cd.load():
                class_name = cd_l.name()
                moddo = cd_l.module()
                try:
                    mod = self.load_module(moddo.name())
                except ModuleNotFoundError:
                    L.warn('Did not find module %s', moddo.name())
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

    def load_class(self, cname_or_mname, cnames=None):
        if cnames:
            mpart = cname_or_mname
        else:
            mpart, cpart = cname_or_mname.rsplit('.', 1)
            cnames = (cpart,)
        m = self.load_module(mpart)
        try:
            res = tuple(self.DecoratedMappedClasses[c]
                        if c in self.DecoratedMappedClasses
                        else c
                        for c in
                        (getattr(m, cname) for cname in cnames))

            return res[0] if len(res) == 1 else res
        except AttributeError:
            raise UnmappedClassException(cnames)

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
        c = self.MappedClasses.get(cname, None)
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
        for c in self.MappedClasses.values():
            yield c

    def __str__(self):
        if self.name is not None:
            return 'Mapper(name="'+str(self.name)+'")'
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
