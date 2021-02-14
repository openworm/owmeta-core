'''
This module defines the root of a high-level interface for owmeta_core, refered to as
"OWM" (for the `main class <OWM>` in the interface), "owm" (for the command line that
wraps the interface), or "the command interface" in the documentation. Additional
"sub-commands" may be defined which provide additional functionality.

If there is a suitable method in the high-level interface, it should generally be
preferred to the lower-level interfaces for stability.
'''
from __future__ import print_function, absolute_import
import sys
from contextlib import contextmanager
import os
from os.path import (exists,
        abspath,
        join as pth_join,
        dirname,
        isabs,
        relpath,
        realpath,
        expanduser,
        expandvars)

from os import makedirs, mkdir, listdir, rename, unlink, scandir

import shutil
import json
import logging
import errno
from collections import namedtuple
from textwrap import dedent
from tempfile import TemporaryDirectory
import uuid

from pkg_resources import iter_entry_points, DistributionNotFound
import rdflib
from rdflib.term import URIRef

from .command_util import (IVar, SubCommand, GeneratorWithData, GenericUserError,
                           DEFAULT_OWM_DIR)
from . import connect, OWMETA_PROFILE_DIR
from .bundle import (BundleDependentStoreConfigBuilder, BundleDependencyManager,
                     retrieve_remotes)
from .commands.bundle import OWMBundle
from .context import (Context, DEFAULT_CONTEXT_KEY, IMPORTS_CONTEXT_KEY,
                      CLASS_REGISTRY_CONTEXT_KEY)
from .context_common import CONTEXT_IMPORTS
from .capability import provide
from .capabilities import FilePathProvider, CacheDirectoryProvider
from .datasource_loader import DataSourceDirLoader, LoadFailed
from .graph_serialization import write_canonical_to_file, gen_ctx_fname
from .mapper import Mapper
from .rdf_utils import BatchAddGraph
from .utils import FCN


L = logging.getLogger(__name__)

DEFAULT_SAVE_CALLABLE_NAME = 'owm_data'
'''
Default name for the provider in the arguments to `OWM.save`
'''

DSDL_GROUP = 'owmeta_core.datasource_dir_loader'


class OWMSourceData(object):
    ''' Commands for saving and loading data for DataSources '''
    def __init__(self, parent):
        self._source_command = parent
        self._owm_command = parent._parent

    def retrieve(self, source, archive='data.tar', archive_type=None):
        '''
        Retrieves the data for the source

        Parameters
        ----------
        source : str
            The source for data
        archive : str
            The file name of the archive. If this ends with an extension like
            '.zip', and no `archive_type` argument is given, then an archive
            will be created of that type. The archive name will *not* have any
            extension appended in any case. optional
        archive_type : str
            The type of the archive to create. optional
        '''
        from owmeta_core.datasource import DataSource
        sid = self._owm_command._den3(source)
        if not archive_type:
            for ext in EXT_TO_ARCHIVE_FMT:
                if archive.endswith(ext):
                    archive_type = EXT_TO_ARCHIVE_FMT.get(ext)
                    break

        if not archive_type:
            if ext:
                msg = "The extension '{}', does not match any known archive format." \
                        " Defaulting to TAR format"
                L.warning(msg.format(ext))
            archive_type = 'tar'

        try:
            sources = self._owm_command._default_ctx.stored(DataSource)(ident=sid).load()
            for data_source in sources:
                dd = self._owm_command._dsd[data_source]
        except KeyError:
            raise GenericUserError('Could not find data for {} ({})'.format(sid, source))

        with self._owm_command._tempdir(prefix='owm-source-data-retrieve.') as d:
            temp_archive = shutil.make_archive(pth_join(d, 'archive'), archive_type, dd)
            rename(temp_archive, archive)


EXT_TO_ARCHIVE_FMT = {
    '.tar.bz2': 'bztar',
    '.tar.gz': 'gztar',
    '.tar.xz': 'xztar',
    '.tar': 'tar',
    '.zip': 'zip',
}


class OWMSource(object):
    ''' Commands for working with DataSource objects '''

    data = SubCommand(OWMSourceData)

    def __init__(self, parent):
        self._parent = parent

    def list(self, context=None, kind=None, full=False):
        """
        List known sources

        Parameters
        ----------
        kind : str
            Only list sources of this kind
        context : str
            The context to query for sources
        full : bool
            Whether to (attempt to) shorten the source URIs by using the namespace manager
        """
        from .datasource import DataSource
        conf = self._parent._conf()
        if context is not None:
            ctx = self._parent._make_ctx(context)
        else:
            ctx = self._parent._default_ctx

        if kind is None:
            kind = DataSource.rdf_type
        kind_uri = self._parent._den3(kind)

        dst = ctx.stored(ctx.stored.resolve_class(kind_uri))
        if dst is None:
            raise GenericUserError('Could not resolve a Python class for ' + str(kind))

        ds = dst.query()
        nm = conf['rdf.graph'].namespace_manager

        def format_id(r):
            if full:
                return r.identifier
            return nm.normalizeUri(r.identifier)

        def format_comment(r):
            comment = r.rdfs_comment()
            if comment:
                return '\n'.join(comment)
            return ''

        return GeneratorWithData(ds.load(),
                                 text_format=format_id,
                                 default_columns=('ID',),
                                 columns=(format_id,
                                          format_comment),
                                 header=('ID', 'Comment'))

    def derivs(self, data_source):
        '''
        List data sources derived from the one given

        Parameters
        -----------
        data_source : str
            The ID of the data source to find derivatives of
        '''
        from owmeta_core.datasource import DataSource
        uri = self._parent._den3(data_source)
        ctx = self._parent._default_ctx.stored
        source = ctx(DataSource)(ident=uri)

        def text_format(dat):
            source, derived = dat
            return '{} → {}'.format(source.identifier, derived.identifier)

        return GeneratorWithData(self._derivs(ctx, source),
                                 text_format=text_format,
                                 header=("Source", "Derived"),
                                 columns=(lambda x: x[0], lambda x: x[1]))

    def _derivs(self, ctx, source):
        from owmeta_core.datasource import DataSource
        derived = ctx(DataSource).query()
        derived.source(source)
        res = []
        for x in derived.load():
            res.append((source, x))
            res += self._derivs(ctx, x)
        return res

    def show(self, *data_source):
        '''
        Parameters
        ----------
        *data_source : str
            The ID of the data source to show
        '''
        from owmeta_core.datasource import DataSource

        for ds in data_source:
            uri = self._parent._den3(ds)
            for x in self._parent._default_ctx.stored(DataSource)(ident=uri).load():
                self._parent.message(x.format_str(stored=True))

    def list_kinds(self, full=False):
        """
        List kinds of DataSources available in the current context.

        Note that *only* DataSource types which are reachable from the current context
        will be listed. So if, for instance, you have just saved some types (e.g., with
        `owm save`) but have not added an import of the contexts for those types, you
        may not see any results from this command.

        Parameters
        ----------
        full : bool
            Whether to (attempt to) shorten the source URIs by using the namespace manager
        """
        from .datasource import DataSource
        from .dataobject import RDFSClass
        from .rdf_query_modifiers import (ZeroOrMoreTQLayer,
                                          rdfs_subclassof_subclassof_zom_creator)
        conf = self._parent._conf()
        ctx = self._parent._default_ctx
        rdfto = ctx.stored(DataSource.rdf_type_object)
        sc = ctx.stored(RDFSClass)()
        sc.rdfs_subclassof_property(rdfto)
        nm = conf['rdf.graph'].namespace_manager
        zom_matcher = rdfs_subclassof_subclassof_zom_creator(DataSource.rdf_type)
        g = ZeroOrMoreTQLayer(zom_matcher, ctx.stored.rdf_graph())
        for x in sc.load(graph=g):
            if full:
                yield x.identifier
            else:
                yield nm.normalizeUri(x.identifier)

    def rm(self, *data_source):
        '''
        Remove a `DataSource`

        Parameters
        ----------
        *data_source : str
            ID of the source to remove
        '''
        import transaction
        from .datasource import DataSource
        with transaction.manager:
            for ds in data_source:
                uri = self._parent._den3(ds)
                ctx = self._parent._default_ctx.stored
                for x in ctx(DataSource).query(ident=uri).load():
                    for trans in x.translation.get():
                        ctx(trans).retract()
                    ctx(x).retract()


class OWMTranslator(object):
    '''
    Data source translator commands
    '''
    def __init__(self, parent):
        self._parent = parent

    def list(self, context=None, full=False):
        '''
        List translators

        Parameters
        ----------
        context : str
            The root context to search
        full : bool
            Whether to (attempt to) shorten the source URIs by using the namespace manager
        '''
        from owmeta_core.datasource import DataTranslator
        if context is not None:
            ctx = self._parent._make_ctx(context)
        else:
            ctx = self._parent._default_ctx
        dt = ctx.stored(DataTranslator).query()
        nm = self._parent.rdf.namespace_manager

        def id_fmt(trans):
            if full:
                return str(trans.identifier)
            else:
                return nm.normalizeUri(trans.identifier)

        return GeneratorWithData(dt.load(), header=('ID',), columns=(id_fmt,),
                text_format=id_fmt)

    def show(self, translator):
        '''
        Show a translator

        Parameters
        ----------
        translator : str
            The translator to show
        '''
        from owmeta_core.datasource import DataTranslator
        conf = self._parent._conf()
        uri = self._parent._den3(translator)
        dt = self._parent._default_ctx.stored(DataTranslator)(ident=uri, conf=conf)
        for x in dt.load():
            self._parent.message(x)
            return

    def create(self, translator_type):
        '''
        Creates an instance of the given translator class and adds it to the graph

        Parameters
        ----------
        translator_type : str
            RDF type for the translator class
        '''
        import transaction

        ctx = self._parent._default_ctx
        translator_uri = self._parent._den3(translator_type)
        translator_cls = ctx.stored.resolve_class(translator_uri)
        if not translator_cls:
            raise GenericUserError(f'Unable to find the class for {translator_type}')
        with transaction.manager:
            ctx(translator_cls)()
            ctx.add_import(translator_cls.definition_context)
            ctx.save()
            ctx.save_imports(transitive=False)

    def list_kinds(self, full=False):
        """
        List kinds of DataTranslators

        Note that *only* DataTranslator types which are reachable from the current context
        will be listed. So if, for instance, you have just saved some types (e.g., with
        `owm save`) but have not added an import of the contexts for those types, you may
        not see any results from this command.

        Parameters
        ----------
        full : bool
            Whether to (attempt to) shorten the translator URIs by using the namespace manager
        """
        from .datasource import DataTranslator
        from .dataobject import RDFSClass
        from .rdf_query_modifiers import (ZeroOrMoreTQLayer,
                                          rdfs_subclassof_subclassof_zom_creator)
        conf = self._parent._conf()
        ctx = self._parent._default_ctx
        rdfto = ctx.stored(DataTranslator.rdf_type_object)
        sc = ctx.stored(RDFSClass)()
        sc.rdfs_subclassof_property(rdfto)
        nm = conf['rdf.graph'].namespace_manager
        zom_matcher = rdfs_subclassof_subclassof_zom_creator(DataTranslator.rdf_type)
        g = ZeroOrMoreTQLayer(zom_matcher, ctx.stored.rdf_graph())
        for x in sc.load(graph=g):
            if full:
                yield x.identifier
            else:
                yield nm.normalizeUri(x.identifier)

    def rm(self, *translator):
        '''
        Remove a `DataTranslator`

        Parameters
        ----------
        *translator : str
            ID of the source to remove
        '''
        import transaction
        from .datasource import DataTranslator
        with transaction.manager:
            for dt in translator:
                uri = self._parent._den3(dt)
                ctx = self._parent._default_ctx.stored
                for x in ctx(DataTranslator).query(ident=uri).load():
                    ctx(x).retract()


class OWMTypes(object):
    '''
    Commands for dealing with Python classes and RDF types
    '''
    def __init__(self, parent):
        self._parent = parent

    def rm(self, *type):
        '''
        Removes info about the given types, like ``rdfs:subClassOf`` statements, and
        removes the corresponding registry entries as well

        Parameters
        ----------
        *type : str
            Types to remove
        '''
        import transaction
        from .dataobject import RDFSClass, RegistryEntry
        with transaction.manager:
            for class_id in type:
                uri = self._parent._den3(class_id)
                ctx = self._parent._default_ctx.stored
                tdo = ctx.stored(RDFSClass)(ident=uri)
                ctx(tdo).retract()

                crctx = self._parent.connect().mapper.class_registry_context
                re = crctx.stored(RegistryEntry).query()
                re.rdf_class(uri)
                for x in re.load():
                    crctx.stored(x).retract()


class OWMNamespace(object):
    '''
    RDF namespace commands
    '''
    def __init__(self, parent):
        self._parent = parent

    def list(self):
        '''
        List namespace prefixes and URIs in the project
        '''
        conf = self._parent._conf()
        nm = conf['rdf.graph'].namespace_manager
        return GeneratorWithData(
                (dict(prefix=prefix, uri=uri)
                    for prefix, uri in nm.namespaces()),
                header=('Prefix', 'URI'),
                columns=(lambda r: r['prefix'],
                         lambda r: r['uri']))


class _ProgressMock(object):

    def __getattr__(self, name):
        return type(self)()

    def __call__(self, *args, **kwargs):
        return type(self)()


class OWMConfig(object):
    '''
    Config file commands
    '''
    user = IVar(value_type=bool,
                default_value=False,
                doc='If set, configs are only for the user; otherwise, they \
                       would be committed to the repository')

    def __init__(self, parent):
        self._parent = parent

    def __setattr__(self, t, v):
        super(OWMConfig, self).__setattr__(t, v)

    @IVar.property('user.conf', value_type=str)
    def user_config_file(self):
        ''' The user config file name '''
        if isabs(self._user_config_file):
            return self._user_config_file
        return pth_join(self._parent.owmdir, self._user_config_file)

    @user_config_file.setter
    def user_config_file(self, val):
        self._user_config_file = val

    def _get_config_file(self):
        if not exists(self._parent.owmdir):
            raise OWMDirMissingException(self._parent.owmdir)

        if self.user:
            res = self.user_config_file
        else:
            res = self._parent.config_file

        if not exists(res):
            if self.user:
                self._init_user_config_file()
            else:
                self._parent._init_config_file()
        return res

    def _init_user_config_file(self):
        with open(self.user_config_file, 'w') as f:
            write_config({}, f)

    def get(self, key):
        '''
        Read a config value

        Parameters
        ----------
        key : str
            The configuration key
        '''
        fname = self._get_config_file()
        with open(fname, 'r') as f:
            ob = json.load(f)
            return ob.get(key)

    def set(self, key, value):
        '''
        Set a config value

        Parameters
        ----------
        key : str
            The configuration key
        value : str
            The value to set
        '''
        fname = self._get_config_file()
        with open(fname, 'r+') as f:
            ob = json.load(f)
            f.seek(0)
            try:
                json_value = json.loads(value)
            except ValueError:
                json_value = value
            ob[key] = json_value
            write_config(ob, f)

    def delete(self, key):
        '''
        Deletes a config value

        Parameters
        ----------
        key : str
            The configuration key
        '''
        fname = self._get_config_file()
        with open(fname, 'r+') as f:
            ob = json.load(f)
            f.seek(0)
            del ob[key]
            write_config(ob, f)


_PROGRESS_MOCK = _ProgressMock()


@contextmanager
def default_progress_reporter(*args, **kwargs):
    yield _PROGRESS_MOCK


POSSIBLE_EDITORS = [
    '/usr/bin/vi',
    '/usr/bin/vim',
    '/usr/bin/nano',
    'vim',
    'vi',
    'nano'
]


class OWMContexts(object):
    '''
    Commands for working with contexts
    '''
    def __init__(self, parent):
        self._parent = parent

    def serialize(self, context=None, destination=None, format='nquads', include_imports=False, whole_graph=False):
        '''
        Serialize the current default context or the one provided

        Parameters
        ----------
        context : str
            The context to save
        destination : file or str
            A file-like object to write the file to or a file name. If not provided, messages the result.
        format : str
            Serialization format (ex, 'n3', 'nquads')
        include_imports : bool
            If true, then include contexts imported by the provided context in the result.
            The default is not to include imported contexts.
        whole_graph : bool
            Serialize all contexts from all graphs (this probably isn't what you want)
        '''

        retstr = False
        if destination is None:
            from six import BytesIO
            retstr = True
            destination = BytesIO()

        if whole_graph:
            if context is not None:
                raise GenericUserError('Serializing the whole graph precludes selecting a'
                        ' single context')
            self._parent.rdf.serialize(destination, format=format)
        else:
            if context is None:
                ctx = self._parent._default_ctx
            else:
                ctx = Context(ident=self._parent._den3(context), conf=self._parent._conf())

            if include_imports:
                ctx.stored.rdf_graph().serialize(destination, format=format)
            else:
                ctx.own_stored.rdf_graph().serialize(destination, format=format)

        if retstr:
            self._parent.message(destination.getvalue().decode(encoding='utf-8'))

    def edit(self, context=None, format=None, editor=None, list_formats=False):
        '''
        Edit a provided context or the current default context.

        The file name of the serialization will be passed as the sole argument to the editor. If the editor argument is
        not provided, will use the EDITOR environment variable. If EDITOR is also not defined, will try a few known
        editors until one is found. The editor must write back to the file.

        Parameters
        ----------
        context : str
            The context to edit
        format : str
            Serialization format (ex, 'n3', 'nquads'). Default 'n3'
        editor : str
            The program which will be used to edit the context serialization.
        list_formats : bool
            List the formats available for editing (I.O.W., formats that we can both read
            and write)
        '''

        from rdflib.plugin import plugins
        from rdflib.serializer import Serializer
        from rdflib.parser import Parser

        serializers = set(x.name for x in plugins(kind=Serializer))
        parsers = set(x.name for x in plugins(kind=Parser))
        formats = serializers & parsers

        if list_formats:
            return formats

        if not format:
            format = 'n3'

        if format not in formats:
            raise GenericUserError("Unsupported format: " + format)

        from subprocess import call
        if context is None:
            ctx = self._parent._default_ctx
            ctxid = self._parent._conf(DEFAULT_CONTEXT_KEY)
        else:
            ctx = Context(ident=context, conf=self._parent._conf())
            ctxid = context

        if not editor:
            editor = os.environ['EDITOR'].strip()

        if not editor:
            for editor in POSSIBLE_EDITORS:
                if hasattr(shutil, 'which'):
                    editor = shutil.which(editor)
                    if editor:
                        break
                elif os.access(editor, os.R_OK | os.X_OK):
                    break

        if not editor:
            raise GenericUserError("No known editor could be found")

        with self._parent._tempdir(prefix='owm-context-edit.') as d:
            from rdflib import plugin
            from rdflib.parser import Parser, create_input_source
            import transaction
            parser = plugin.get(format, Parser)()
            fname = pth_join(d, 'data')
            with transaction.manager:
                with open(fname, mode='wb') as destination:
                    # For canonical graphs, we would need to sort the triples first,
                    # but it's not needed here -- the user probably doesn't care one
                    # way or the other
                    ctx.own_stored.rdf_graph().serialize(destination, format=format)
                call([editor, fname])
                with open(fname, mode='rb') as source:
                    g = self._parent.own_rdf.get_context(ctxid)
                    g.remove((None, None, None))
                    parser.parse(create_input_source(source), g)

    def list(self, include_dependencies=False):
        '''
        List the set of contexts in the graph

        Parameters
        ----------
        include_dependencies : bool
            If set, then contexts from dependencies will be included
        '''
        if include_dependencies:
            for c in self._parent.rdf.contexts():
                yield c.identifier
        else:
            for c in self._parent.own_rdf.contexts():
                yield c.identifier

    def list_changed(self):
        '''
        Return the set of contexts which differ from the serialization on disk
        '''
        return self._parent._changed_contexts_set()

    def list_imports(self, context):
        '''
        List the contexts that the given context imports

        Parameters
        ----------
        context : str
            The context to list imports for
        '''
        ctx = self._parent._make_ctx(context).stored
        for c in ctx.imports:
            yield c.identifier

    def list_importers(self, context):
        '''
        List the contexts that import the given context

        Parameters
        ----------
        context : str
            The context to list importers for
        '''
        imports_ctxid = self._parent.imports_context()
        imports_ctx = self._parent._context(Context)(imports_ctxid).stored

        g = imports_ctx.rdf_graph()
        for t in g.triples((None, CONTEXT_IMPORTS, URIRef(context))):
            yield t[0]

    def add_import(self, importer, imported):
        '''
        Add an import to the imports graph

        Parameters
        ----------
        importer : str
            The importing context
        imported : list str
            The imported context
        '''
        import transaction

        importer_ctx = self._parent._context(Context)(importer)
        for imp in imported:
            importer_ctx.add_import(Context(imp))
        with transaction.manager:
            importer_ctx.save_imports()

    def rm_import(self, importer, imported):
        '''
        Remove an import statement

        Parameters
        ----------
        importer : str
            The importing context
        imported : list of str
            An imported context
        '''
        import transaction
        imports_ctxid = self._parent.imports_context()
        imports_ctx = self._parent._context(Context)(imports_ctxid).stored
        with transaction.manager:
            for imp in imported:
                imports_ctx.rdf_graph().remove((URIRef(importer), CONTEXT_IMPORTS, URIRef(imp)))

    def bundle(self, context):
        '''
        Show the closest bundle that defines this context

        Parameters
        ----------
        context : str
            The context to lookup
        '''
        context = self._parent._den3(context)
        with self._parent.connect():
            dep_mgr = self._parent._bundle_dep_mgr
            contexts = set(str(getattr(c, 'identifier', c)) for c in self._parent.own_rdf.contexts())
            target_bundle = dep_mgr.lookup_context_bundle(contexts, str(context))
            if target_bundle is dep_mgr:
                return None
            return target_bundle

    def rm(self, *context):
        '''
        Remove a context

        Parameters
        ----------
        *context : str
            Context to remove
        '''
        import transaction
        graph = self._parent.own_rdf
        with transaction.manager:
            for c in context:
                c = self._parent._den3(c)
                graph.remove_graph(c)


class OWMRegistry(object):
    '''
    Commands for dealing with the class registry, a mapping of RDF types to classes in
    imperative programming languages
    '''

    def __init__(self, parent):
        self._parent = parent

    def list(self, module=None, rdf_type=None, class_name=None):
        '''
        List registered classes

        Parameters
        ----------
        module : str
            If provided, limits the registry entries returned to those that have the given
            module name. Optional.
        rdf_type : str
            If provided, limits the registry entries returned to those that have the given
            RDF type. Optional.
        class_name : str
            If provided, limits the registry entries returned to those that have the given
            class name. Optional.
        '''
        from .dataobject import PythonClassDescription
        mapper = self._parent.connect().mapper

        def registry_entries():
            for re in mapper.load_registry_entries():
                ident = re.identifier
                cd = re.class_description()
                re_rdf_type = re.rdf_class()
                if not isinstance(cd, PythonClassDescription):
                    continue
                module_do = cd.module()
                if re.namespace_manager:
                    ident = re.namespace_manager.normalizeUri(ident)
                if hasattr(module_do, 'name'):
                    module_name = module_do.name()
                re_class_name = cd.name()

                if module is not None and module != module_name:
                    continue

                if rdf_type is not None and rdf_type != str(re_rdf_type):
                    continue

                if class_name is not None and class_name != str(re_class_name):
                    continue

                res = dict(id=ident,
                        rdf_type=re_rdf_type,
                        class_name=re_class_name,
                        module_name=module_name)

                if hasattr(module_do, 'package'):
                    package = module_do.package()
                    if package:
                        res['package'] = dict(id=package.identifier,
                                              name=package.name(),
                                              version=package.version())
                yield res

        def fmt_text(entry, format=None):
            if format == 'pretty':
                pkg_id = entry.get('package') and entry['package']['id']
                return dedent('''\
                {id}:
                    RDF Type: {rdf_type}
                    Module Name: {module_name}
                    Class Name: {class_name}
                    Package: {pkg_id}\n''').format(pkg_id=pkg_id, **entry)
            else:
                return entry['id']

        return GeneratorWithData(registry_entries(),
                header=('ID', 'RDF Type', 'Class Name', 'Module Name', 'Package',
                        'Package Name', 'Package Version'),
                columns=(lambda r: r['id'],
                         lambda r: r['rdf_type'],
                         lambda r: r['class_name'],
                         lambda r: r['module_name'],
                         lambda r: r.get('package') and r['package']['id'],
                         lambda r: r.get('package') and r['package']['name'],
                         lambda r: r.get('package') and r['package']['version'],),
                default_columns=('ID', 'RDF Type', 'Class Name', 'Module Name', 'Package'),
                text_format=fmt_text)

    def show(self, *registry_entry):
        '''
        Show registry entries

        Parameters
        ----------
        *registry_entry : str
            Registry entry to show
        '''

    def rm(self, *registry_entry):
        '''
        Remove a registry entry

        Parameters
        ----------
        *registry_entry : str
            Registry entry to remove
        '''
        import transaction
        from .dataobject import RegistryEntry

        with transaction.manager:
            for re in registry_entry:
                uri = self._parent._den3(re)
                crctx = self._parent.connect().mapper.class_registry_context
                for x in crctx(RegistryEntry).query(ident=uri).load():
                    crctx.stored(x).retract()


class OWM(object):
    """
    High-level commands for working with owmeta data
    """

    graph_accessor_finder = IVar(doc='Finds an RDFLib graph from the given URL')

    basedir = IVar('.', doc='The base directory. owmdir is resolved against this base')

    repository_provider = IVar(doc='The provider of the repository logic'
                                   ' (cloning, initializing, committing, checkouts)')

    non_interactive = IVar(value_type=bool,
            doc='If this option is provided, then interactive prompts are not allowed')

    context = IVar(doc='Context to use instead of the default context. Commands that'
            ' work with other contexts (e.g., `owm contexts rm-import`) will continue'
            ' to use those other contexts unless otherwise indicated')

    # N.B.: Sub-commands are created on-demand when you access the attribute,
    # hence they do not, in any way, store attributes set on them. You must
    # save the instance of the subcommand to a variable in order to make
    # multiple statements including that sub-command
    config = SubCommand(OWMConfig)

    source = SubCommand(OWMSource)

    translator = SubCommand(OWMTranslator)

    namespace = SubCommand(OWMNamespace)

    contexts = SubCommand(OWMContexts)

    type = SubCommand(OWMTypes)

    bundle = SubCommand(OWMBundle)

    registry = SubCommand(OWMRegistry)

    def __init__(self, owmdir=None, non_interactive=False):
        self.progress_reporter = default_progress_reporter
        self.message = lambda *args, **kwargs: print(*args, **kwargs)

        def prompt(*args, **kwargs):
            res = input(*args, **kwargs)
            print()
            return res
        self.prompt = prompt

        self._data_source_directories = None
        self._changed_contexts = None
        self._owm_connection = None
        self._context_change_tracker = None

        if owmdir:
            self.owmdir = owmdir

        if non_interactive:
            self.non_interactive = non_interactive

        self._bundle_dep_mgr = None
        self._context = _ProjectContext(owm=self)

    @IVar.property(OWMETA_PROFILE_DIR)
    def userdir(self):
        '''
        Root directory for user-specific configuration
        '''
        return realpath(expandvars(expanduser(self._userdir)))

    @userdir.setter
    def userdir(self, val):
        self._userdir = val

    @IVar.property(DEFAULT_OWM_DIR)
    def owmdir(self):
        '''
        The base directory for owmeta files. The repository provider's files also go under here
        '''
        if isabs(self._owmdir):
            res = self._owmdir
        else:
            res = pth_join(self.basedir, self._owmdir)
        return res

    @owmdir.setter
    def owmdir(self, val):
        self._owmdir = val

    @IVar.property('owm.conf', value_type=str)
    def config_file(self):
        ''' The config file name '''
        if isabs(self._config_file):
            return self._config_file
        return pth_join(self.owmdir, self._config_file)

    @config_file.setter
    def config_file(self, val):
        self._config_file = val

    @IVar.property('worm.db')
    def store_name(self):
        ''' The file name of the database store '''
        if isabs(self._store_name):
            return self._store_name
        return pth_join(self.owmdir, self._store_name)

    @store_name.setter
    def store_name(self, val):
        self._store_name = val

    def _ensure_owmdir(self):
        if not exists(self.owmdir):
            makedirs(self.owmdir)

    @IVar.property
    def log_level(self):
        ''' Log level '''
        return logging.getLevelName(logging.getLogger().getEffectiveLevel())

    @log_level.setter
    def log_level(self, level):
        logging.getLogger().setLevel(getattr(logging, level.upper()))

    def save(self, module, provider=None, context=None):
        '''
        Save the data in the given context

        Saves the "mapped" classes declared in a module and saves the objects declared by
        the "provider" (see the argument's description)

        Parameters
        ----------
        module : str
            Name of the module housing the provider
        provider : str
            Name of the provider, a callble that accepts a context object and adds
            statements to it. Can be a "dotted" name indicating attribute accesses.
            Default is `DEFAULT_SAVE_CALLABLE_NAME`
        context : str
            The target context. The default context is used
        '''
        import transaction
        import importlib as IM
        from functools import wraps
        conf = self._conf()

        added_cwd = False
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.append(cwd)
            added_cwd = True

        try:
            mod = IM.import_module(module)
            provider_not_set = provider is None
            if not provider:
                provider = DEFAULT_SAVE_CALLABLE_NAME

            conn = self.connect()
            if not context:
                ctx = conn(_OWMSaveContext)(self._default_ctx, mod)
            else:
                ctx = conn(_OWMSaveContext)(Context(ident=context, conf=conf), mod)
            attr_chain = provider.split('.')
            prov = mod
            for x in attr_chain:
                try:
                    prov = getattr(prov, x)
                except AttributeError:
                    if provider_not_set and getattr(mod, '__yarom_mapped_classes__', None):
                        def prov(*args, **kwargs):
                            pass
                        break
                    raise
            ns = OWMSaveNamespace(context=ctx)

            mapped_classes = getattr(mod, '__yarom_mapped_classes__', None)
            if mapped_classes:
                # It's a module with class definitions -- take each of the mapped
                # classes and add their contexts so they're saved properly...
                orig_prov = prov
                mapper = self._owm_connection.mapper

                @wraps(prov)
                def save_classes(ns):
                    ns.include_context(mapper.class_registry_context)
                    mapper.process_module(module, mod)
                    mapper.declare_python_class_registry_entry(*mapped_classes)
                    for mapped_class in mapped_classes:
                        ns.include_context(mapped_class.definition_context)
                    orig_prov(ns)
                prov = save_classes

            with transaction.manager:
                prov(ns)
                ns.save(graph=conf['rdf.graph'])
            return ns.created_contexts()
        finally:
            if added_cwd:
                sys.path.remove(cwd)

    def say(self, subject, property, object):
        '''
        Make a statement

        Parameters
        ----------
        subject : str
            The object which you want to say something about
        property : str
            The type of statement to make
        object : str
            The other object you want to say something about
        '''
        from owmeta_core.dataobject import BaseDataObject
        import transaction
        dctx = self._default_ctx
        query = dctx.stored(BaseDataObject)(ident=self._den3(subject))
        with transaction.manager:
            for ob in query.load():
                getattr(dctx(ob), property)(object)
            dctx.save()

    def set_default_context(self, context, user=False):
        '''
        Set current default context for the repository

        Parameters
        ----------
        context : str
            The context to set
        user : bool
            If set, set the context only for the current user. Has no effect for
            retrieving the context
        '''
        config = self.config
        config.user = user
        config.set(DEFAULT_CONTEXT_KEY, context)

    def get_default_context(self):
        '''
        Read the current target context for the repository
        '''
        return self._conf().get(DEFAULT_CONTEXT_KEY)

    def imports_context(self, context=None, user=False):
        '''
        Read or set current target imports context for the repository

        Parameters
        ----------
        context : str
            The context to set
        user : bool
            If set, set the context only for the current user. Has no effect for
            retrieving the context
        '''
        if context is not None:
            config = self.config
            config.user = user
            config.set(IMPORTS_CONTEXT_KEY, context)
        else:
            return self._conf().get(IMPORTS_CONTEXT_KEY)

    def init(self, update_existing_config=False, default_context_id=None):
        """
        Makes a new graph store.

        The configuration file will be created if it does not exist. If it
        *does* exist, the location of the database store will, by default, not
        be changed in that file

        If not provided, some values will be prompted for, unless batch (non-interactive)
        mode is enabled. If batch mode is enabled, either an error will be returned or a
        default value will be used for missing options. Values which are required either
        in a prompt or as options are indicated as "Required" below.

        Parameters
        ----------
        update_existing_config : bool
            If True, updates the existing config file to point to the given
            file for the store configuration
        default_context_id : str
            URI for the default context. Required
        """
        try:
            reinit = exists(self.owmdir)
            self._ensure_owmdir()
            if not exists(self.config_file):
                self._init_config_file(default_context_id=default_context_id)
            elif update_existing_config:
                with open(self.config_file, 'r+') as f:
                    conf = json.load(f)
                    conf['rdf.store_conf'] = pth_join('$OWM',
                            relpath(abspath(self.store_name), abspath(self.owmdir)))
                    f.seek(0)
                    write_config(conf, f)

            self._init_store()
            self._init_repository(reinit)
            if reinit:
                self.message('Reinitialized owmeta-core project at %s' % abspath(self.owmdir))
            else:
                self.message('Initialized owmeta-core project at %s' % abspath(self.owmdir))
        except BaseException:
            if not reinit:
                self._ensure_no_owmdir()
            raise

    def _ensure_no_owmdir(self):
        if exists(self.owmdir):
            shutil.rmtree(self.owmdir)

    def _init_config_file(self, default_context_id=None):
        with open(self._default_config_file_name(), 'r') as f:
            default = json.load(f)
            with open(self.config_file, 'w') as of:
                default['rdf.store_conf'] = pth_join('$OWM',
                        relpath(abspath(self.store_name), abspath(self.owmdir)))

                if not default_context_id and not self.non_interactive:
                    default_context_id = self.prompt(dedent('''\
                    The default context is where statements are placed by default. The URI
                    for this context should use a domain name that you control.

                    Please provide the URI of the default context: '''))

                default_context_id = default_context_id and str(default_context_id).strip()
                if not default_context_id:
                    raise GenericUserError("A default context ID is required")

                default[DEFAULT_CONTEXT_KEY] = str(default_context_id).strip()

                default[IMPORTS_CONTEXT_KEY] = str(uuid.uuid4().urn).strip()

                default[CLASS_REGISTRY_CONTEXT_KEY] = str(uuid.uuid4().urn).strip()

                write_config(default, of)

    def _init_repository(self, reinit):
        if self.repository_provider is not None:
            self.repository_provider.init(base=self.owmdir)
            if not reinit:
                self.repository_provider.add([relpath(self.config_file, self.owmdir)])
                self.repository_provider.commit('Initial commit')

    def _den3(self, s):
        if not s:
            return s
        from rdflib.namespace import is_ncname
        conf = self._conf()
        nm = conf['rdf.graph'].namespace_manager
        if s.startswith('<') and s.endswith('>'):
            return URIRef(s.strip(u'<>'))
        parts = s.split(':')
        if len(parts) > 1 and is_ncname(parts[1]):
            for pref, ns in nm.namespaces():
                if pref == parts[0]:
                    s = URIRef(ns + parts[1])
                    break
        return URIRef(s)

    def fetch_graph(self, url):
        """
        Fetch a graph

        Parameters
        ----------
        url : str
            URL for the graph
        """
        res = self._obtain_graph_accessor(url)
        if not res:
            raise UnreadableGraphException('Could not read the graph at {}'.format(url))
        return res()

    def add_graph(self, url=None, context=None, include_imports=True):
        """
        Fetch a graph and add it to the local store.

        Parameters
        ----------
        url : str
            The URL of the graph to fetch
        context : rdflib.term.URIRef
            If provided, only this context and, optionally, its imported graphs
            will be added.
        include_imports : bool
            If True, imports of the named context will be included. Has no
            effect if context is None.
        """
        graph = self.fetch_graph(url)
        dat = self._conf()

        dat['rdf.graph'].addN(graph.quads((None, None, None, context)))

    def _obtain_graph_accessor(self, url):
        if self.graph_accessor_finder is None:
            raise Exception('No graph_accessor_finder has been configured')

        return self.graph_accessor_finder(url)

    def connect(self, read_only=False):
        self._init_store(read_only=read_only)
        return _ProjectConnection(self, self._owm_connection)

    def _conf(self, *args, read_only=False):
        from owmeta_core.data import Data
        import six
        dat = getattr(self, '_dat', None)
        if not dat or self._dat_file != self.config_file:
            if not exists(self.config_file):
                raise NoConfigFileError(self.config_file)

            with open(self.config_file) as repo_config:
                rc = json.load(repo_config)
            if not exists(self.config.user_config_file):
                uc = {}
            else:
                with open(self.config.user_config_file) as user_config:
                    uc = json.load(user_config)

            # Pre-process the user-config to resolve variables based on the user
            # config-file location
            uc['configure.file_location'] = self.config.user_config_file
            udat = Data.process_config(uc, variables={'OWM': self.owmdir})

            rc.update(udat.items())
            rc['configure.file_location'] = self.config_file
            dat = Data.process_config(rc, variables={'OWM': self.owmdir})
            dat['owm.directory'] = self.owmdir
            store_conf = dat.get('rdf.store_conf', None)
            if not store_conf:
                raise GenericUserError('rdf.store_conf is not defined in either of the OWM'
                ' configuration files at ' + self.config_file + ' or ' +
                self.config.user_config_file + ' OWM repository may have been initialized'
                ' incorrectly')
            if (isinstance(store_conf, six.string_types) and
                    isabs(store_conf) and
                    not store_conf.startswith(abspath(self.owmdir))):
                raise GenericUserError('rdf.store_conf must specify a path inside of ' +
                        self.owmdir + ' but instead it is ' + store_conf)

            deps = dat.get('dependencies', None)
            if deps:
                bundles_directory = self.bundle._bundles_directory()
                remotes_directory = self.bundle._user_remotes_directory()
                project_remotes = list(retrieve_remotes(self.bundle._project_remotes_directory()))
                # XXX: Look at how we bring in projects remotes directory
                cfg_builder = BundleDependentStoreConfigBuilder(bundles_directory=bundles_directory,
                                                                remotes_directory=remotes_directory,
                                                                remotes=project_remotes,
                                                                read_only=read_only)
                store_name, store_conf = cfg_builder.build(store_conf, deps)
                dat['rdf.source'] = 'default'
                dat['rdf.store'] = store_name
                dat['rdf.store_conf'] = store_conf
                self._bundle_dep_mgr = BundleDependencyManager(bundles_directory=bundles_directory,
                                                               remotes_directory=remotes_directory,
                                                               remotes=project_remotes,
                                                               dependencies=lambda: deps)

            self._owm_connection = connect(conf=dat)

            self._dat = dat
            self._dat_file = self.config_file
        if args:
            return dat.get(*args)
        return dat

    _init_store = _conf

    def disconnect(self):
        from owmeta_core import disconnect
        if self._owm_connection is not None:
            disconnect(self._owm_connection)
            self._dat = None
            self._owm_connection = None

    def clone(self, url=None, update_existing_config=False, branch=None):
        """Clone a data store

        Parameters
        ----------
        url : str
            URL of the data store to clone
        update_existing_config : bool
            If True, updates the existing config file to point to the given
            file for the store configuration
        branch : str
            Branch to checkout after cloning
        """
        try:
            makedirs(self.owmdir)
            self.message('Cloning...', file=sys.stderr)
            with self.progress_reporter(file=sys.stderr, unit=' objects', miniters=0) as progress:
                self.repository_provider.clone(url, base=self.owmdir,
                        progress=progress, branch=branch)
            if not exists(self.config_file):
                self._init_config_file()
            self._init_store()
            self.message('Deserializing...', file=sys.stderr)
            self._regenerate_database()
            self.message('Done!', file=sys.stderr)
        except FileExistsError:
            raise
        except BaseException:
            self._ensure_no_owmdir()
            raise

    def git(self, *args):
        '''
        Runs git commmands in the ".owm" directory

        Parameters
        ----------
        *args
            arguments to git
        '''
        from subprocess import Popen, PIPE
        startdir = os.getcwd()
        try:
            os.chdir(self.owmdir)
        except FileNotFoundError:
            raise GenericUserError('Cannot find ".owm" directory')

        try:
            with Popen(['git'] + list(args), stdout=PIPE) as p:
                self.message(p.stdout.read().decode('utf-8', 'ignore'))
        finally:
            os.chdir(startdir)

    def regendb(self):
        '''
        Regenerates the indexed database from graph serializations.

        Note that any uncommitted contents in the indexed database will be deleted.
        '''
        from glob import glob
        for g in glob(self.store_name + '*'):
            self.message('unlink', g)
            try:
                unlink(g)
            except IsADirectoryError:
                shutil.rmtree(g)

        self._regenerate_database()

    def _regenerate_database(self):
        with self.progress_reporter(unit=' ctx', file=sys.stderr) as ctx_prog, \
                self.progress_reporter(unit=' triples', file=sys.stderr, leave=False) as trip_prog:
            self._load_all_graphs(ctx_prog, trip_prog)

    def _load_all_graphs(self, progress, trip_prog):
        import transaction
        from rdflib import plugin
        from rdflib.parser import Parser, create_input_source
        idx_fname = pth_join(self.owmdir, 'graphs', 'index')
        triples_read = 0
        if exists(idx_fname):
            dest = self.rdf
            with open(idx_fname) as index_file:
                cnt = 0
                for l in index_file:
                    cnt += 1
                index_file.seek(0)
                progress.total = cnt
                with transaction.manager:
                    bag = BatchAddGraph(dest, batchsize=10000)
                    for l in index_file:
                        fname, ctx = l.strip().split(' ')
                        parser = plugin.get('nt', Parser)()
                        graph_fname = pth_join(self.owmdir, 'graphs', fname)
                        with open(graph_fname, 'rb') as f, bag.get_context(ctx) as g:
                            parser.parse(create_input_source(f), g)

                        progress.update(1)
                        trip_prog.update(bag.count - triples_read)
                        triples_read = g.count
                    progress.write('Finalizing writes to database...')
        progress.write('Loaded {:,} triples'.format(triples_read))

    def _graphs_index(self):
        idx_fname = pth_join(self.owmdir, 'graphs', 'index')
        if exists(idx_fname):
            with open(idx_fname) as index_file:
                for ent in self._graphs_index0(index_file):
                    yield ent

    @property
    def _context_fnames(self):
        if not hasattr(self, '_cfn'):
            self._read_graphs_index()
        return self._cfn

    @property
    def _fname_contexts(self):
        if not hasattr(self, '_fnc'):
            self._read_graphs_index()
        return self._fnc

    def _read_graphs_index(self):
        ctx_index = dict()
        fname_index = dict()
        for fname, ctx in self._graphs_index():
            ctx_index[ctx] = pth_join(self.owmdir, 'graphs', fname)
            fname_index[fname] = ctx
        self._cfn = ctx_index
        self._fnc = fname_index

    def _read_graphs_index0(self, index_file):
        ctx_index = dict()
        fname_index = dict()
        for fname, ctx in self._graphs_index0(index_file):
            ctx_index[ctx] = fname
            fname_index[fname] = ctx
        return ctx_index, fname_index

    def _graphs_index0(self, index_file):
        for l in index_file.readlines():
            l = l.strip()
            if not isinstance(l, str):
                l_str = l.decode('UTF-8')
            else:
                l_str = l
            yield l_str.split(' ')

    def translate(self, translator, output_key=None, output_identifier=None,
                  data_sources=(), named_data_sources=None):
        """
        Do a translation with the named translator and inputs

        Parameters
        ----------
        translator : str or `.DataTranslator`
            Translator identifier
        output_key : str
            Output key. Used for generating the output's identifier. Exclusive with output_identifier
        output_identifier : str
            Output identifier. Exclusive with output_key
        data_sources : list of str or list of `.DataSource`
            Input data sources
        named_data_sources : dict
            Named input data sources
        """
        import transaction

        if named_data_sources is None:
            named_data_sources = dict()

        translator_obj = self._lookup_translator(translator)
        if translator_obj is None:
            raise GenericUserError(f'No translator for {translator}')

        positional_sources = [self._lookup_source(src) for src in data_sources]
        if None in positional_sources:
            raise GenericUserError(f'No source for "{data_sources[positional_sources.index(None)]}"')
        named_sources = {k: self._lookup_source(src) for k, src in
                named_data_sources.items()}
        with self._tempdir(prefix='owm-translate.') as d:
            orig_wd = os.getcwd()
            with transaction.manager:
                os.chdir(d)
                try:
                    res = self._default_ctx(translator_obj)(*positional_sources,
                                         output_identifier=output_identifier,
                                         output_key=output_key,
                                         **named_sources)
                finally:
                    os.chdir(orig_wd)
                res.commit()
                res.context.save_context()
                return res

    @contextmanager
    def _tempdir(self, *args, **kwargs):
        td = pth_join(self.owmdir, 'temp')
        if not exists(td):
            makedirs(td)
        kwargs['dir'] = td
        with TemporaryDirectory(*args, **kwargs) as d:
            yield d

    @property
    def _dsd(self):
        self._load_data_source_directories()
        return self._data_source_directories

    def _load_data_source_directories(self):
        if not self._data_source_directories:
            # The DSD holds mappings to data sources we've loaded before. In general, this
            # allows the individual loaders to not worry about checking if they have
            # loaded something before.

            # XXX persist the dict
            loaders = [OWMDirDataSourceDirLoader()]
            for entry_point in iter_entry_points(group=DSDL_GROUP):
                try:
                    loaders.append(entry_point.load()())
                except DistributionNotFound:
                    L.debug('Not adding DataSource directory loader %s due to failure in'
                            ' package resources resolution',
                            entry_point, exc_info=True)
            dsd = _DSD(dict(), pth_join(self.owmdir, 'data_source_data'), loaders)
            try:
                dindex = open(pth_join(self.owmdir, 'data_source_directories'))
                for ds_id, dname in (x.strip().split(' ', 1) for x in dindex):
                    dsd.put(ds_id, dname)
            except OSError:
                pass
            self._data_source_directories = dsd

    def _stage_translation_directory(self, source_directory, target_directory):
        self.message('Copying files into {} from {}'.format(target_directory, source_directory))
        # TODO: Add support for a selector based on a MANIFEST and/or ignore
        # file to pass in as the 'ignore' option to copytree
        for dirent in listdir(source_directory):
            src = pth_join(source_directory, dirent)
            dst = pth_join(target_directory, dirent)
            self.message('Copying {} to {}'.format(src, dst))
            try:
                shutil.copytree(src, dst)
            except OSError as e:
                if e.errno == errno.ENOTDIR:
                    shutil.copy2(src, dst)

    def _lookup_translator(self, tname):
        from owmeta_core.datasource import DataTranslator

        if isinstance(tname, DataTranslator):
            tname = tname.identifier

        for x in self._default_ctx.stored(DataTranslator)(ident=self._den3(tname)).load():
            return x

    def _lookup_source(self, sname):
        from owmeta_core.datasource import DataSource

        if isinstance(sname, DataSource):
            sname = sname.identifier

        for x in self._default_ctx.stored(DataSource)(ident=self._den3(sname)).load():
            provide(x, self._cap_provs())
            return x

    def _cap_provs(self):
        return [DataSourceDirectoryProvider(self._dsd),
                WorkingDirectoryProvider(),
                OWMCacheDirectoryProvider(pth_join(self.owmdir, 'cache'))]

    @property
    def _default_ctx(self):
        context = None
        if self.context:
            context = self.context
        else:
            conf = self._conf()
            try:
                context = conf[DEFAULT_CONTEXT_KEY]
            except KeyError:
                raise ConfigMissingException(DEFAULT_CONTEXT_KEY)

        return Context.contextualize(self._context)(ident=context)

    default_context = _default_ctx

    def _make_ctx(self, ctxid=None):
        return Context.contextualize(self._context)(ident=ctxid)

    def _package_path(self):
        """
        Get the package path
        """
        from pkgutil import get_loader
        return dirname(get_loader('owmeta_core').get_filename())

    def _default_config_file_name(self):
        return pth_join(self._package_path(), 'default.conf')

    def list_contexts(self):
        '''
        List contexts
        '''
        for m in self.contexts.list():
            yield m

    @property
    def rdf(self):
        return self._conf('rdf.graph')

    @property
    def own_rdf(self):
        has_dependencies = self._conf('dependencies', None)
        if has_dependencies:
            return rdflib.Dataset(
                    self._conf('rdf.graph').store.stores[0],
                    default_union=True)
        else:
            return self._conf('rdf.graph')

    def commit(self, message):
        '''
        Write the graph to the local repository

        Parameters
        ----------
        message : str
            commit message
        '''
        repo = self.repository_provider
        self._serialize_graphs()
        repo.commit(message)

    def _changed_contexts_set(self):
        gf_index = {URIRef(y): x for x, y in self._graphs_index()}
        gfkeys = set(gf_index.keys())
        return gfkeys

    def _serialize_graphs(self, ignore_change_cache=False):
        import transaction
        g = self.own_rdf
        repo = self.repository_provider

        repo.base = self.owmdir
        graphs_base = pth_join(self.owmdir, 'graphs')

        changed = self._changed_contexts_set()

        if changed or repo.is_dirty:
            repo.reset()

        if not exists(graphs_base):
            mkdir(graphs_base)

        files = []
        ctx_data = []
        deleted_contexts = dict(self._context_fnames)
        with transaction.manager:
            for context in g.contexts():
                if not context:
                    continue
                ident = context.identifier

                if not ignore_change_cache:
                    ctx_changed = ident in changed
                else:
                    ctx_changed = True

                sfname = self._context_fnames.get(str(ident))
                if not sfname:
                    fname = gen_ctx_fname(ident, graphs_base)
                else:
                    fname = sfname

                # If there's a context in the graph, but we don't even have a file, then it is changed.
                # This can happen if we get out of sync with what's on disk.
                if not ctx_changed and not exists(fname):
                    ctx_changed = True

                if ctx_changed:
                    # N.B. We *overwrite* changes to the serialized graphs -- the source of truth is what's in the
                    # RDFLib graph unless we regenerate the database
                    write_canonical_to_file(context, fname)
                ctx_data.append((relpath(fname, graphs_base), ident))
                files.append(fname)
                deleted_contexts.pop(str(ident), None)

        index_fname = pth_join(graphs_base, 'index')
        with open(index_fname, 'w') as index_file:
            for l in sorted(ctx_data):
                print(*l, file=index_file, end='\n')

        if deleted_contexts:
            repo.remove(relpath(f, self.owmdir) for f in deleted_contexts.values())
            for f in deleted_contexts.values():
                unlink(f)

        files.append(index_fname)
        repo.add([relpath(f, self.owmdir) for f in files] + [relpath(self.config_file, self.owmdir)])

    def diff(self, color=False):
        """
        Show differences between what's in the working context set and what's in the serializations

        Parameters
        ----------
        color : bool
            If set, then ANSI color escape codes will be incorporated into diff output.
            Default is to output without color.
        """
        from difflib import unified_diff
        from os.path import basename

        r = self.repository_provider
        try:
            self._serialize_graphs(ignore_change_cache=False)
        except Exception:
            r.reset()
            L.exception("Could not serialize graphs")
            raise GenericUserError("Could not serialize graphs")

        head_commit = r.repo().head.commit

        # TODO: Determine if this path should actually be platform-dependent
        try:
            old_index = head_commit.tree.join(pth_join('graphs', 'index'))
        except KeyError:
            old_index = None

        if old_index:
            # OStream.stream isn't documented (most things in GitDB aren't), but it is,
            # technically, public interface.
            old_index_file = old_index.data_stream.stream
            _, old_fnc = self._read_graphs_index0(old_index_file)
        else:
            old_fnc = dict()

        new_index_filename = pth_join(self.owmdir, 'graphs', 'index')
        try:
            with open(new_index_filename, 'r') as new_index_file:
                _, new_fnc = self._read_graphs_index0(new_index_file)
        except FileNotFoundError:
            new_fnc = dict()

        di = head_commit.diff(None)

        for d in di:
            try:
                a_blob = d.a_blob
                if a_blob:
                    adata = a_blob.data_stream.read().split(b'\n')
                else:
                    adata = []
            except Exception as e:
                print('No "a" data: {}'.format(e), file=sys.stderr)
                adata = []

            try:
                b_blob = d.b_blob
                if b_blob:
                    bdata = b_blob.data_stream.read().split(b'\n')
                else:
                    with open(pth_join(r.repo().working_dir, d.b_path), 'rb') as f:
                        bdata = f.read().split(b'\n')
            except Exception as e:
                print('No "b" data: {}'.format(e), file=sys.stderr)
                bdata = []
            afname = basename(d.a_path)
            bfname = basename(d.b_path)

            graphdir = pth_join(self.owmdir, 'graphs')
            if not adata:
                fromfile = '/dev/null'
            else:
                fromfile = old_fnc.get(afname, afname)

            if not bdata:
                tofile = '/dev/null'
            else:
                tofile = new_fnc.get(bfname, bfname)

            try:
                diff = unified_diff([x.decode('utf-8') + '\n' for x in adata],
                                    [x.decode('utf-8') + '\n' for x in bdata],
                                    fromfile='a ' + fromfile,
                                    tofile='b ' + tofile,
                                    lineterm='\n')
                if color:
                    diff = self._colorize_diff(diff)

                sys.stdout.writelines(diff)
            except Exception:
                if adata and not bdata:
                    sys.stdout.writelines('Deleted ' + fromfile + '\n')
                elif bdata and not adata:
                    sys.stdout.writelines('Created ' + fromfile + '\n')
                else:
                    asize = a_blob.size
                    asha = a_blob.hexsha
                    bsize = b_blob.size
                    bsha = b_blob.hexsha
                    diff = dedent('''\
                    --- a {fromfile}
                    --- Size: {asize}
                    --- Shasum: {asha}
                    +++ b {tofile}
                    +++ Size: {bsize}
                    +++ Shasum: {bsha}''').format(locals())
                    if color:
                        diff = self._colorize_diff(diff)
                    sys.stdout.writelines(diff)

    def _colorize_diff(self, lines):
        from termcolor import colored
        import re
        hunk_line_numbers_pattern = re.compile(r'^@@[0-9 +,-]+@@')
        for l in lines:
            l = l.rstrip()
            if l.startswith('+++') or l.startswith('---'):
                l = colored(l, attrs=['bold'])
            elif hunk_line_numbers_pattern.match(l):
                l = colored(l, 'cyan')
            elif l.startswith('+'):
                l = colored(l, 'green')
            elif l.startswith('-'):
                l = colored(l, 'red')
            l += os.linesep
            yield l


class _ProjectConnection(object):

    def __init__(self, owm, connection):
        self.owm = owm
        self.connection = connection
        self._context = owm._context

    @property
    def mapper(self):
        # XXX: Maybe refactor this...
        return self._context.mapper

    def __getattr__(self, attr):
        return getattr(self.connection, attr)

    def __call__(self, o):
        return self._context(o)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.owm.disconnect()


class _ProjectContext(Context):
    '''
    `Context` for a project.
    '''
    def __init__(self, *args, owm, **kwargs):
        super().__init__(*args, **kwargs)
        self.owm = owm
        self._mapper = None

    @property
    def conf(self):
        return self.owm._conf()

    @property
    def mapper(self):
        if self._mapper is None:
            self._mapper = _ProjectMapper(owm=self.owm)
        return self._mapper


class _ProjectMapper(Mapper):
    def __init__(self, owm):
        owm_conf = owm._conf()
        super().__init__(name=f'{owm.owmdir}', conf=owm_conf)
        self.owm = owm
        self._resolved_classes = dict()

    def resolve_class(self, rdf_type, context):
        prev_resolved_class = self._resolved_classes.get((rdf_type, context.identifier))
        if prev_resolved_class:
            return prev_resolved_class

        own_resolved_class = super().resolve_class(rdf_type, context)

        if own_resolved_class:
            self._resolved_classes[(rdf_type, context.identifier)] = own_resolved_class
            return own_resolved_class

        target_id = context.identifier
        dep_mgr = self.owm._bundle_dep_mgr
        if dep_mgr:
            contexts = set(str(getattr(c, 'identifier', c)) for c in self.owm.own_rdf.contexts())
            target_bundle = dep_mgr.lookup_context_bundle(contexts, target_id)
            if target_bundle is None:
                target_bundle = dep_mgr
            deps = target_bundle.load_dependencies_transitive()
            for bnd in deps:
                crctx_id = bnd.manifest_data.get(CLASS_REGISTRY_CONTEXT_KEY, None)
                if not crctx_id:
                    continue
                with bnd:
                    resolved_class = bnd.connection.mapper.resolve_class(rdf_type, context)
                    if resolved_class:
                        self._resolved_classes[(rdf_type, context.identifier)] = resolved_class
                        return resolved_class
        return None


class WorkingDirectoryProvider(FilePathProvider):
    '''
    Provides file paths from the current working directory for
    `.data_trans.local_file_ds.LocalFileDataSource` instances.
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cwd = os.getcwd()

    def provides_to(self, obj):
        from owmeta_core.data_trans.local_file_ds import LocalFileDataSource
        file_name = obj.file_name.one()
        if not file_name:
            return None
        if (isinstance(obj, LocalFileDataSource) and
                exists(pth_join(self.cwd, file_name))):
            return self
        return None

    def file_path(self):
        return self.cwd


class OWMCacheDirectoryProvider(CacheDirectoryProvider):
    '''
    Provides a directory in the OWM project directory for caching remote resources as
    local files
    '''

    def __init__(self, cache_directory, **kwargs):
        super().__init__(**kwargs)
        self._cache_directory = cache_directory

    def provides_to(self, obj):
        return self

    def cache_directory(self, cache_key):
        res = pth_join(self._cache_directory, cache_key)
        makedirs(res)
        return res


class _OWMSaveContext(Context):

    def __init__(self, backer, user_module=None):
        self._user_mod = user_module
        self._backer = backer  #: Backing context
        self._imported_ctx_ids = set([self._backer.identifier])
        self._unvalidated_statements = []

    def add_import(self, ctx):
        self._imported_ctx_ids.add(ctx.identifier)
        # Remove unvalidated statements which had this new context as the one they are missing
        self._unvalidated_statements = [p for p in self._unvalidated_statements
                                        if isinstance(p.validation_record, UnimportedContextRecord) and
                                        p.validation_record.context != ctx.identifier]
        return self._backer.add_import(ctx)

    def add_statement(self, stmt):
        stmt_tuple = (stmt.subject, stmt.property, stmt.object)

        def gen():
            for i, x in enumerate(stmt_tuple):
                if (x.context is not None and
                        x.context.identifier is not None and
                        x.context.identifier not in self._imported_ctx_ids):
                    yield UnimportedContextRecord(self._backer.identifier,
                                                  x.context.identifier,
                                                  i,
                                                  stmt)

            for i, x in enumerate(stmt_tuple):
                if (x.context is not None and
                        x.context.identifier is None):
                    yield NullContextRecord(i, stmt)

        for record in gen():
            from inspect import getouterframes, currentframe
            self._unvalidated_statements.append(SaveValidationFailureRecord(self._user_mod,
                                                                            getouterframes(currentframe()),
                                                                            record))
        return self._backer.add_statement(stmt)

    def __getattr__(self, name):
        return getattr(self._backer, name)

    def save_context(self, *args, **kwargs):
        return self._backer.save_context(*args, **kwargs)

    def save_imports(self, *args, **kwargs):
        return self._backer.save_imports(*args, **kwargs)


def write_config(ob, f):
    json.dump(ob, f, sort_keys=True, indent=4, separators=(',', ': '))
    f.write('\n')
    f.truncate()


class InvalidGraphException(GenericUserError):
    ''' Thrown when a graph cannot be translated due to formatting errors '''


class UnreadableGraphException(GenericUserError):
    ''' Thrown when a graph cannot be read due to it being missing, the active user lacking permissions, etc. '''


class NoConfigFileError(GenericUserError):
    '''
    Thrown when a project config file (e.g., '.owm/owm.conf') cannot be found
    '''

    def __init__(self, config_file_path):
        super(NoConfigFileError, self).__init__('Cannot find config file at "%s"' %
                config_file_path)


class OWMDirMissingException(GenericUserError):
    '''
    Thrown when the .owm directory is needed, but cannot be found
    '''


class SaveValidationFailureRecord(namedtuple('_SaveValidationFailureRecord', ['user_module',
                                                                              'stack',
                                                                              'validation_record'])):
    '''
    Record of a validation failure in `OWM.save`
    '''
    def filtered_stack(self):
        umfile = getattr(self.user_module, '__file__', None)
        if umfile and umfile.endswith('pyc'):
            umfile = umfile[:-3] + 'py'
        ourfile = __file__

        if ourfile.endswith('pyc'):
            ourfile = ourfile[:-3] + 'py'

        def find_last_user_frame(frames):
            start = False
            lastum = 0
            res = []
            for i, f in enumerate(frames):
                if umfile and f[1].startswith(umfile):
                    lastum = i
                if start:
                    res.append(f)
                if not start and f[1].startswith(ourfile):
                    start = True
            return res[:lastum]

        return find_last_user_frame(self.stack)

    def __str__(self):
        from traceback import format_list
        stack = format_list([x[1:4] + (''.join(x[4]).strip(),) for x in reversed(self.filtered_stack())])
        fmt = '{}\n Traceback (most recent call last, outer owmeta_core frames omitted):\n {}'
        res = fmt.format(self.validation_record, '\n '.join(''.join(s for s in stack if s).split('\n')))
        return res.strip()


class _DSD(object):
    def __init__(self, ds_dict, base_directory, loaders):
        self._dsdict = ds_dict
        self.base_directory = base_directory
        self._loaders = self._init_loaders(loaders)

    def __str__(self):
        return '{}({})'.format(FCN(type(self)), self._dsdict)

    def __getitem__(self, data_source):
        dsid = str(data_source.identifier)
        try:
            return self._dsdict[dsid]
        except KeyError:
            res = self._load_data_source(data_source)
            if res:
                self._dsdict[dsid] = res
                return res
            raise

    def put(self, data_source_ident, directory):
        self._dsdict[str(data_source_ident)] = directory

    def _init_loaders(self, loaders):
        res = []
        for loader in loaders:
            nd = pth_join(self.base_directory, loader.directory_key)
            if not exists(nd):
                makedirs(nd)
            loader.base_directory = nd
            res.append(loader)
        return res

    def _load_data_source(self, data_source):
        for loader in self._loaders:
            if loader.can_load(data_source):
                return loader(data_source)


class DataSourceDirectoryProvider(FilePathProvider):
    def __init__(self, dsd):
        self._dsd = dsd

    def provides_to(self, ob):
        try:
            path = self._dsd[ob]
        except KeyError:
            return None

        return _DSDP(path)


class _DSDP(FilePathProvider):
    def __init__(self, path):
        self._path = path

    def file_path(self):
        return self._path


class OWMDirDataSourceDirLoader(DataSourceDirLoader):
    def __init__(self, *args, **kwargs):
        super(OWMDirDataSourceDirLoader, self).__init__(*args, **kwargs)
        self._index = dict()

    @property
    def _idx_fname(self):
        if self.base_directory:
            return pth_join(self.base_directory, 'index')
        return None

    def _load_index(self):
        with scandir(self.base_directory) as dirents:
            dentdict = {de.name: de for de in dirents}
            with open(self._idx_fname) as f:
                for l in f:
                    dsid, dname = l.strip().split(' ')
                    if self._index_dir_entry_is_bad(dname, dentdict.get(dname)):
                        continue

                    self._index[dsid] = dname

    def _index_dir_entry_is_bad(self, dname, de):
        if not de:
            msg = "There is no directory entry for {} in {}"
            L.warning(msg.format(dname, self.base_directory), exc_info=True)
            return True

        if not de.is_dir():
            msg = "The directory entry for {} in {} is not a directory"
            L.warning(msg.format(dname, self.base_directory))
            return True

        return False

    def _ensure_index_loaded(self):
        if not self._index:
            self._load_index()

    def can_load(self, data_source):
        try:
            self._ensure_index_loaded()
        except (OSError, IOError) as e:
            # If the index file just happens not to be here since the repo doesn't have any data source directories,
            # then we just can't load the data source's data, but for any other kind of error, something more exotic
            # could be the cause, so let the caller handle it
            #
            if e.errno == 2: # FileNotFound
                return False
            raise
        return str(data_source.identifier) in self._index

    def load(self, data_source):
        try:
            self._ensure_index_loaded()
        except Exception as e:
            raise LoadFailed(data_source, self, "Failed to load the index: " + str(e))

        try:
            return self._index[str(data_source.identifier)]
        except KeyError:
            raise LoadFailed(data_source, self, 'The given identifier is not in the index')


class OWMSaveNamespace(object):
    def __init__(self, context):
        self.context = context
        self._created_ctxs = set()
        self._external_contexts = set()

    def new_context(self, ctx_id):
        # Get the type of our context contextualized *with* our context
        ctx_type = self.context(type(self.context))

        # Make the "backing" context for the result we return
        new_ctx = self.context(Context)(ident=ctx_id, conf=self.context.conf)

        # Make the "wrapper" context and pass through the user's module for validation
        res = ctx_type(new_ctx, user_module=self.context._user_mod)

        # Finally, add the context
        self._created_ctxs.add(res)
        return res

    def include_context(self, ctx):
        '''
        Include the given exernally-created context for saving.

        If the context is being made within the save function, then you can use new_context instead.
        '''
        self._external_contexts.add(ctx)

    def created_contexts(self):
        for ctx in self._created_ctxs:
            yield ctx
        yield self.context

    def validate(self):
        unvalidated = []
        for c in self._created_ctxs:
            unvalidated += c._unvalidated_statements
        unvalidated += self.context._unvalidated_statements
        if unvalidated:
            raise StatementValidationError(unvalidated)

    def save(self, *args, **kwargs):
        # TODO: (openworm/owmeta#374) look at automatically importing contexts based
        # on UnimportedContextRecords among SaveValidationFailureRecords
        self.validate()
        for c in self._created_ctxs:
            c.save_context(*args, **kwargs)
            c.save_imports(*args, **kwargs)
        for c in self._external_contexts:
            c.save_context(*args, **kwargs)
            self.context(c).save_imports(*args, **kwargs)
        self.context.save_imports(*args, **kwargs)

        return self.context.save_context(*args, **kwargs)


class NullContextRecord(namedtuple('_NullContextRecord', ['node_index', 'statement'])):
    '''
    Stored when the identifier for the context of an object we're saving is `None`
    '''

    def __str__(self):
        from .rdf_utils import triple_to_n3
        trip = self.statement.to_triple()
        fmt = 'Context identifier is `None` for {} of statement "{}"'
        return fmt.format(trip[self.node_index].n3(),
                          triple_to_n3(trip))


class UnimportedContextRecord(namedtuple('_UnimportedContextRecord',
                                         ['importer', 'context', 'node_index', 'statement'])):
    '''
    Stored when statements include a reference to an object but do not include the
    context of that object in the callback passed to `OWM.save`. For example, if we had a
    callback like this::

        def owm_data(ns):
            ctxA = ns.new_context(ident='http://example.org/just-pizza-stuff')
            ctxB = ns.new_context(ident='http://example.org/stuff-sam-likes')
            sam = ctxB(Person)('sam')
            pizza = ctxA(Thing)('pizza')
            sam.likes(pizza)

    it would generate this error because ``ctxB`` does not declare an import for ``ctxA``
    '''

    def __str__(self):
        from .rdf_utils import triple_to_n3
        trip = self.statement.to_triple()
        fmt = 'Missing import of context {} from {} for {} of statement "{}"'
        return fmt.format(self.context.n3(),
                          self.importer.n3(),
                          trip[self.node_index].n3(),
                          triple_to_n3(trip))


class StatementValidationError(GenericUserError):
    '''
    Thrown in the case that a set of statements fails to validate
    '''
    def __init__(self, statements):
        msgfmt = '{} invalid statements were found:\n{}'
        msg = msgfmt.format(len(statements), '\n'.join(str(x) for x in statements))
        super(StatementValidationError, self).__init__(msg)
        self.statements = statements


class ConfigMissingException(GenericUserError):
    '''
    Thrown when a configuration key is missing
    '''
    def __init__(self, key):
        super(ConfigMissingException, self).__init__(
                'Missing "%s" in configuration' % key)
        self.key = key
