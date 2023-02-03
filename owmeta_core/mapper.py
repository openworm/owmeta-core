import importlib as IM
import logging

from .dataobject import (BaseDataObject, DataObject, RegistryEntry,
                         PythonClassDescription, Module, PythonModule, ClassDescription,
                         ClassResolutionFailed, ModuleResolutionFailed)
from .utils import FCN
from .configure import Configurable


# TODO: Move this into mapper or a new mapper_common module
CLASS_REGISTRY_CONTEXT_KEY = 'class_registry_context_id'
'''
.. confval:: class_registry_context_id

    Configuration file key for the URI of the class registry RDF graph context.

    The class registry context holds the mappings between RDF types and Python classes for
    a project or bundle.
'''

CLASS_REGISTRY_CONTEXT_LIST_KEY = 'class_registry_context_list'
'''
.. confval:: class_registry_context_list

    Configuration file key for the list of class registry contexts

    If it is specified, then :confval:`class_registry_context_id` should be searched first
    for class registry entries. The class registry list may be built automatically or not
    defined at all depending on who makes the `Configuration`, but if it is specified with
    this property, then it should be respected.
'''

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
    Keeps track of relationships between Python classes and RDF classes

    The mapping this object manages may also be written to the RDF graph as `class
    registry entries <RegistryEntry>`. The entries are written to the "class registry
    context", which can be specified when the Mapper is created.
    '''
    def __init__(self, name=None, class_registry_context=None,
            class_registry_context_list=None, **kwargs):
        '''
        Parameters
        ----------
        name : str, optional
            Name of the mapper for diagnostic/debugging purposes
        class_registry_context : `owmeta_core.context.Context` or str, optional
            The context where mappings should be saved and/or retrieved from. Either the
            context object itself or the ID for it. If not provided, then the class
            registry context ID is looked up from the Mapper's configuration at
            `CLASS_REGISTRY_CONTEXT_KEY`
        class_registry_context_list : list of `owmeta_core.context.Context` or str, optional
            List of contexts or context IDs where registry entries should be retrieved
            from if the class_registry_context doesn't yield a mapping
        **kwargs
            passed to super-classes
        '''
        super(Mapper, self).__init__(**kwargs)

        # Maps full class names (i.e., including the module name) to classes
        self._mapped_classes = dict()

        # Maps RDF types to properties of the related class
        self._rdf_type_table = dict()

        # Modules that have already been loaded
        self.modules = dict()

        if name is None:
            name = hex(id(self))
        self.name = name

        self.__class_registry_context_id = None
        self.__class_registry_context = None
        if isinstance(class_registry_context, str):
            self.__class_registry_context_id = class_registry_context
        else:
            self.__class_registry_context = class_registry_context

        self.__class_registry_context_id_list = None
        self.__class_registry_context_list = []
        if class_registry_context_list:
            if isinstance(class_registry_context_list[0], str):
                self.__class_registry_context_id_list = class_registry_context_list
            else:
                self.__class_registry_context_list = class_registry_context_list

        self._bootstrap_mappings()

    @property
    def class_registry_context(self):
        ''' Context where class registry entries are stored '''
        if self.__class_registry_context is None:
            from .context import Context
            crctx_id = (self.__class_registry_context_id
                    or self.conf.get(CLASS_REGISTRY_CONTEXT_KEY, None))
            if crctx_id is None:
                return None
            crctx = Context(crctx_id, conf=self.conf, mapper=self)
            self.__class_registry_context = crctx
        return self.__class_registry_context

    @property
    def class_registry_context_list(self):
        '''
        Context where class registry entries are retrieved from if
        `class_registry_context` doesn't contain an appropriate entry
        '''
        if self.__class_registry_context_list is None:
            from .context import Context
            crctx_ids = (self.__class_registry_context_id_list
                    or self.conf.get(CLASS_REGISTRY_CONTEXT_LIST_KEY, None))
            if crctx_ids is None:
                return []
            crctxs = []
            for crctx_id in crctx_ids:
                crctxs.append(Context(crctx_id, conf=self.conf, mapper=self).stored)
            self.__class_registry_context_list = crctxs
        return (([self.class_registry_context.stored]
                if self.class_registry_context else []) +
                self.__class_registry_context_list)

    def _bootstrap_mappings(self):
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
        return self.modules.get(module_name, None)

    def _check_is_good_class_registry(self, cls):
        module = IM.import_module(cls.__module__)
        if hasattr(module, cls.__name__):
            return

        ymc = getattr(module, '__yarom_mapped_classes__', None)
        if ymc and cls in ymc:
            return

        L.warning(('While saving the registry entry of {}, we found that its'
                  ' module, {}, does not have "{}" in its'
                  ' namespace').format(cls, cls.__module__, cls.__name__))

    def save(self):
        crctx = self.class_registry_context
        if crctx is None:
            raise Exception(f'{self}.class_registry_context is unset.'
                    ' Cannot save class registry entries')
        self.declare_python_class_registry_entry(*self._rdf_type_table.values())
        crctx.save()
        crctx.save_imports()

    def declare_python_class_registry_entry(self, *classes):
        crctx = self.class_registry_context
        if crctx is None:
            raise Exception(f'{self}.class_registry_context is unset.'
                    ' Cannot declare class registry entries')
        for cls in classes:
            crctx(cls).declare_class_registry_entry()

    def load_registry_entries(self):
        crctx = self.class_registry_context.stored
        return crctx(RegistryEntry)().load()

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

        if self.class_registry_context is None:
            L.warning('%s.class_registry_context is unset.'
                    ' Cannot resolve class for "%s"',
                    self, uri)
            return None
        resolved_class = None
        for crctx in self.class_registry_context_list:
            resolved_class = self._resolve_class(uri, crctx)
            if resolved_class:
                break

        if resolved_class:
            self.add_class(resolved_class)
        return resolved_class

    def _resolve_class(self, uri, crctx):
        re = crctx(RegistryEntry)()
        re.rdf_class(uri)
        cd = crctx(PythonClassDescription)()
        re.class_description(cd)
        c = None

        for cd_l in cd.load():
            try:
                c = cd_l.resolve_class()
            except ClassResolutionFailed as e:
                if isinstance(e.__cause__, ModuleResolutionFailed):
                    L.warn('_resolve_class: Did not find module', exc_info=True)
                    continue
            if c is not None:
                break

            # Fall-back class resolution
            class_name = cd_l.name()
            if class_name is None:
                L.warning('_resolve_class: Could not find a class name attached to'
                          ' %s', cd_l)
                continue
            moddo = cd_l.module()
            if moddo is None:
                # Try loading the module using the more generic ClassDescription:module
                # relationship instead. It's acceptable as long as the type is
                # PythonModule
                L.warning('_resolve_class: Could not find a module attached via'
                          ' PythonClassDescription:module to %s for the class named %s.'
                          ' Trying ClassDescription:module instead...', cd_l, class_name)
                moddo = ClassDescription.module(cd_l)()
                if not isinstance(moddo, PythonModule):
                    L.warning('_resolve_class: Could not find a module attached to'
                              ' %s for the class named %s', cd_l, class_name)
                    continue
            mod = moddo.resolve_module()
            L.warning('_resolve_class: Did not find class %s in %s', class_name, mod.__name__)
            ymc = getattr(mod, '__yarom_mapped_classes__', None)
            if not ymc:
                L.warning('_resolve_class: No __yarom_mapped_classes__ in %s, so cannot look up %s',
                        mod.__name__, class_name)
                continue

            matching_classes = tuple(mc for mc in ymc
                                     if mc.__name__ == class_name)
            if not matching_classes:
                L.warning('_resolve_class: Did not find class %s in %s.__yarom_mapped_classes__',
                        class_name, mod.__name__)
                continue

            c = matching_classes[0]
            if len(matching_classes) > 1:
                L.warning('_resolve_class: More than one class has the same name in'
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
        return self._mapped_classes.get(cname, None)

    def mapped_classes(self):
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
