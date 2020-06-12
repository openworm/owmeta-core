from collections import namedtuple
from itertools import chain
from os import makedirs, rename, scandir, listdir
from os.path import (join as p, exists, relpath, isdir, isfile,
        expanduser, expandvars, realpath)
from struct import pack
import errno
import hashlib
import json
import logging
import re
import shutil

from rdflib.term import URIRef
import six
from textwrap import dedent
import transaction
import yaml

from .. import OWMETA_PROFILE_DIR
from ..context import DEFAULT_CONTEXT_KEY, IMPORTS_CONTEXT_KEY, Context
from ..context_common import CONTEXT_IMPORTS
from ..data import Data
from ..file_match import match_files
from ..file_lock import lock_file
from ..file_utils import hash_file
from ..graph_serialization import write_canonical_to_file
from ..rdf_utils import transitive_lookup
from ..utils import FCN, aslist

from .archive import Unarchiver
from .common import (find_bundle_directory, fmt_bundle_directory, BUNDLE_MANIFEST_FILE_NAME,
                     BUNDLE_INDEXED_DB_NAME, validate_manifest, BUNDLE_MANIFEST_VERSION)
from .exceptions import (NotADescriptor, BundleNotFound, NoRemoteAvailable, NoBundleLoader,
                         NotABundlePath, NoAcceptableUploaders,
                         FetchTargetIsNotEmpty, TargetIsNotEmpty, UncoveredImports)

from urllib.parse import quote as urlquote, unquote as urlunquote


L = logging.getLogger(__name__)

DEFAULT_BUNDLES_DIRECTORY = p(OWMETA_PROFILE_DIR, 'bundles')
'''
Default directory for the bundle cache
'''

DEFAULT_REMOTES_DIRECTORY = p(OWMETA_PROFILE_DIR, 'remotes')
'''
Default directory for descriptors of user-level remotes as opposed to project-specific
remotes
'''


class Remote(object):
    '''
    A place where bundles come from and go to
    '''
    def __init__(self, name, accessor_configs=()):
        '''
        Parameters
        ----------
        name : str
            The name of the remote
        accessor_configs : iterable of AccessorConfig
            Configs for how you access the remote
        '''

        self.name = name
        ''' Name of the remote '''

        self.accessor_configs = list(accessor_configs)
        '''
        Configs for how you access the remote.

        One might configure mirrors or replicas for a given bundle repository as multiple
        accessor configs
        '''

    def add_config(self, accessor_config):
        '''
        Add the given accessor config to this remote

        Parameters
        ----------
        accessor_config : AccessorConfig
            The config to add

        Returns
        -------
        bool
            `True` if the accessor config was added (meaning there's no equivalent one
            already set for this remote). Otherwise, `False`.
        '''
        if accessor_config in self.accessor_configs:
            return False
        self.accessor_configs.append(accessor_config)
        return True

    def generate_loaders(self):
        '''
        Generate the bundle loaders for this remote.

        Loaders are generated from `accessor_configs` and `LOADER_CLASSES` according with
        which type of `Loader` can load a type of accessor
        '''
        for ac in self.accessor_configs:
            for lc in LOADER_CLASSES:
                if lc.can_load_from(ac):
                    loader = lc(ac)
                    yield loader

    def generate_uploaders(self):
        '''
        Generate the bundle uploaders for this remote
        '''
        for ac in self.accessor_configs:
            for uc in UPLOADER_CLASSES:
                if uc.can_upload_to(ac):
                    loader = uc(ac)
                    yield loader

    def write(self, out):
        '''
        Serialize the `Remote` and write to `out`

        Parameters
        ----------
        out : :term:`file object`
            Target for writing the remote
        '''
        yaml.dump(self, out)

    @classmethod
    def read(cls, inp):
        '''
        Read a serialized `Remote`

        Parameters
        ----------
        inp : :term:`file object`
            File-like object containing the serialized `Remote`
        '''
        res = yaml.full_load(inp)
        assert isinstance(res, cls)
        return res

    def __eq__(self, other):
        return (self.name == other.name and
                self.accessor_configs == other.accessor_configs)

    def __hash__(self):
        return hash((self.name, self.accessor_configs))

    def __str__(self):
        if self.accessor_configs:
            accessors = '\n' + '\n'.join('    ' + '\n    '.join(str(acc).split('\n')) for acc in self.accessor_configs)
        else:
            accessors = ' <none>'
        return dedent('''\
        {name}
        Accessors:{accessors}''').format(name=self.name,
            accessors=accessors)

    def __repr__(self):
        return f'{FCN(type(self))}({repr(self.name)}, {repr(self.accessor_configs)})'


class DependencyDescriptor(namedtuple('_DependencyDescriptor',
        ('id', 'version', 'excludes'))):
    __slots__ = ()

    def __new__(cls, id, version=None, excludes=()):
        return super(DependencyDescriptor, cls).__new__(cls, id, version, excludes)


class AccessorConfig(object):
    '''
    Configuration for accessing a `Remote`. `Loaders <Loader>` are added to a remote according to
    which accessors are avaialble
    '''

    def __eq__(self, other):
        raise NotImplementedError()

    def __hash__(self):
        raise NotImplementedError()


class _DepList(list):
    def add(self, dd):
        self.append(dd)


class URLConfig(AccessorConfig):
    '''
    Configuration for accessing a remote with just a URL.
    '''

    def __init__(self, url):
        self.url = url

    def __eq__(self, other):
        return isinstance(other, URLConfig) and self.url == other.url

    def __hash__(self):
        return hash(self.url)

    def __str__(self):
        return '{}(url={})'.format(FCN(type(self)), repr(self.url))

    @classmethod
    def register(cls, scheme):
        URL_CONFIG_MAP[scheme] = cls

    __repr__ = __str__


URL_CONFIG_MAP = {}
'''
`URLConfigs <URLConfig>` by scheme. Can be populated by pkg_resources entry points
'''


class Descriptor(object):
    '''
    Descriptor for a bundle.

    The descriptor is sufficient to build a distributable bundle directory tree from a
    `~rdflib.graph.ConjunctiveGraph` and a set of files (see `Installer`).
    '''
    def __init__(self, ident, **kwargs):
        self.id = ident
        self._set(kwargs)

    @classmethod
    def make(cls, obj):
        '''
        Makes a descriptor from the given object.

        Parameters
        ----------
        obj : a `dict-like object <dict>`
            An object with parameters for the Descriptor. Typically a dict

        Returns
        -------
        Descriptor
            The created descriptor
        '''
        res = cls(ident=obj['id'])
        res._set(obj)
        return res

    @classmethod
    def load(cls, descriptor_source):
        '''
        Load a descriptor from a YAML record

        Parameters
        ----------
        descriptor_source : str or :term:`file object`
            The descriptor source. Handled by `yaml.safe_load
            <https://pyyaml.org/wiki/PyYAMLDocumentation#the-yaml-package>`_

        Raises
        ------
        NotADescriptor
            Thrown when the object loaded from `descriptor_source` isn't a `dict`
        '''
        dat = yaml.safe_load(descriptor_source)
        if isinstance(dat, dict):
            return cls.make(dat)
        else:
            raise NotADescriptor()

    def _set(self, obj):
        self.name = obj.get('name', self.id)
        self.version = obj.get('version', 1)
        self.description = obj.get('description', None)
        self.patterns = set(make_pattern(x) for x in obj.get('patterns', ()))
        self.includes = set(make_include_func(x) for x in obj.get('includes', ()))
        self.empties = {uri for uri, options in (inc.popitem()
            for inc in obj.get('includes', ())
            if isinstance(inc, dict))
            if options.get('empty', False) is True}

        deps_set = set()
        deps = _DepList()
        for x in obj.get('dependencies', ()):
            if isinstance(x, six.string_types):
                dd = DependencyDescriptor(x)
            elif isinstance(x, dict):
                dd = DependencyDescriptor(**x)
            else:
                dd = DependencyDescriptor(*x)
            if dd not in deps_set:
                deps.append(dd)
                deps_set.add(dd)
        self.dependencies = deps
        self.files = FilesDescriptor.make(obj.get('files', None))

    def __str__(self):
        return (FCN(type(self)) + '(ident={},'
                'name={},version={},description={},'
                'patterns={},includes={},'
                'files={},dependencies={})').format(
                        repr(self.id),
                        repr(self.name),
                        repr(self.version),
                        repr(self.description),
                        repr(self.patterns),
                        repr(self.includes),
                        repr(self.files),
                        repr(self.dependencies))


class Bundle(object):
    '''
    Main entry point for using bundles

    Typical usage is something like this::

        >>> from owmeta_core.dataobject import DataObject
        >>> with Bundle('example/bundleId', version=42) as bnd:
        ...     for do in bnd(DataObject).load():
        ...         # Do something with `do`
        ...         pass
    '''

    def __init__(self, ident, bundles_directory=DEFAULT_BUNDLES_DIRECTORY, version=None,
            conf=None, remotes=None, remotes_directory=DEFAULT_REMOTES_DIRECTORY):
        '''
        .. note::

            Paths, `bundles_directory` and `remotes_directory`, will have symbolic links,
            environment variables, and "~" (for the current user's home directory)
            expanded when the `Bundle` is initialized. To reflect changes to symbolic
            links or home directories, the `bundles_directory` or `remotes_directory`
            attributes must be updated directly or a new instance must be created.

        Parameters
        ----------
        ident : str
            Bundle ID
        bundles_directory : str, optional
            Path to the bundles directory. Defaults to `.DEFAULT_BUNDLES_DIRECTORY`
        version : int, optional
            Bundle version to access. By default, the latest version will be used.
        conf : .Configuration or dict, optional
            Configuration to add to the one created for the bundle automatically. Values
            for the default imports context (`.IMPORTS_CONTEXT_KEY`), the default context
            (`.DEFAULT_CONTEXT_KEY`) and store (``'rdf.store'``, ``'rdf.source'``, and,
            ``'rdf.store_conf'``) will be ignored and overwritten.
        remotes : iterable of Remote or str, optional
            A subset of remotes and additional remotes to fetch from. See `Fetcher.fetch`
        remotes_directory : str, optional
            The directory to load `Remotes <Remote>` from in case a bundle is not in the
            bundle cache. Defaults to `.DEFAULT_REMOTES_DIRECTORY`
        '''
        if not ident or not isinstance(ident, str):
            raise ValueError('ident must be a non-empty string')
        self.ident = ident
        if not bundles_directory:
            bundles_directory = DEFAULT_BUNDLES_DIRECTORY
        self.bundles_directory = realpath(expandvars(expanduser(bundles_directory)))
        if not conf:
            conf = {}

        conf.update({'rdf.source': 'default'})
        self.version = version
        self.remotes = remotes
        if not remotes_directory:
            remotes_directory = DEFAULT_REMOTES_DIRECTORY
        self.remotes_directory = realpath(expandvars(expanduser(remotes_directory)))
        self._given_conf = conf
        self.conf = None
        self._contexts = None

    @property
    def identifier(self):
        return self.ident

    def resolve(self):
        try:
            bundle_directory = self._get_bundle_directory()
        except BundleNotFound:
            # If there's a .owm directory, then get the remotes from there
            f = Fetcher(self.bundles_directory, retrieve_remotes(self.remotes_directory))
            bundle_directory = f.fetch(self.ident, self.version, self.remotes)
        return bundle_directory

    def _get_bundle_directory(self):
        # - look up the bundle in the bundle cache
        # - generate a config based on the current config load the config
        # - make a database from the graphs, if necessary (similar to `owm regendb`). If
        #   delete the existing database if it doesn't match the store config
        return find_bundle_directory(self.bundles_directory, self.ident, self.version)

    def initdb(self):
        '''
        Initialize the bundle's `conf` `~owmeta_core.data.Data` instance
        '''
        if self.conf is None:
            bundle_directory = self.resolve()
            self.conf = Data().copy(self._given_conf)
            self.conf[IMPORTS_CONTEXT_KEY] = fmt_bundle_ctx_id(self.ident)
            with open(p(bundle_directory, BUNDLE_MANIFEST_FILE_NAME)) as mf:
                manifest_data = json.load(mf)
                self.conf[DEFAULT_CONTEXT_KEY] = manifest_data.get(DEFAULT_CONTEXT_KEY)
                self.conf[IMPORTS_CONTEXT_KEY] = manifest_data.get(IMPORTS_CONTEXT_KEY)
            self.conf['rdf.store'] = 'agg'
            self.conf['rdf.store_conf'] = self._construct_store_config(
                    bundle_directory,
                    manifest_data)
            self.conf.init()

    def _construct_store_config(self, bundle_directory, manifest_data, current_path=None, paths=None):
        if paths is None:
            paths = set()
        if current_path is None:
            current_path = _BDTD()
        dependency_configs = self._gather_dependency_configs(manifest_data, current_path, paths)
        indexed_db_path = p(bundle_directory, BUNDLE_INDEXED_DB_NAME)
        fs_store_config = dict(url=indexed_db_path, read_only=True)
        return [
            ('FileStorageZODB', fs_store_config)
        ] + dependency_configs

    @aslist
    def _gather_dependency_configs(self, manifest_data, current_path, paths):
        for dd in manifest_data.get('dependencies', ()):
            dep_path = current_path.merge_excludes(dd.get('excludes', ()))
            if (dep_path, (dd['id'], dd.get('version'))) in paths:
                return
            paths.add((dep_path, (dd['id'], dd.get('version'))))
            bundle_directory = find_bundle_directory(self.bundles_directory, dd['id'], dd.get('version'))
            with open(p(bundle_directory, BUNDLE_MANIFEST_FILE_NAME)) as mf:
                manifest_data = json.load(mf)
            # We don't want to include items in the configuration that aren't specified by
            # the dependency descriptor. Also, all of the optionals have defaults that
            # BundleDependencyStore handles itself, so we don't want to impose them here.
            addl_dep_confs = {k: v for k, v in dd.items()
                    if k in ('excludes',) and v}
            yield ('owmeta_core_bds', dict(type='agg',
                        conf=self._construct_store_config(bundle_directory, manifest_data,
                            dep_path, paths),
                        **addl_dep_confs))

    @property
    def contexts(self):
        ''' Return contexts in a bundle '''
        # Since bundles are meant to be immutable, we won't need to add
        if self._contexts is not None:
            return self._contexts
        bundle_directory = self.resolve()
        contexts = set()
        graphs_directory = p(bundle_directory, 'graphs')
        idx_fname = p(graphs_directory, 'index')
        if not exists(idx_fname):
            raise Exception('Cannot find an index at {}'.format(repr(idx_fname)))
        with open(idx_fname, 'rb') as index_file:
            for l in index_file:
                l = l.strip()
                if not l:
                    continue
                ctx, _ = l.split(b'\x00')
                contexts.add(ctx.decode('UTF-8'))
        self._contexts = contexts
        return self._contexts

    @property
    def rdf(self):
        self.initdb()
        return self.conf['rdf.graph']

    def __enter__(self):
        self.initdb()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Close the database connection
        self.conf.destroy()
        self.conf = None

    def __call__(self, target):
        if target and hasattr(target, 'contextualize'):
            ctx = BundleContext(None, conf=self.conf)
            return target.contextualize(ctx.stored)
        return target


class _BDTD(namedtuple('_BDTD', ('excludes',))):
    '''
    Bundle Dependency Traversal Data (BDTD)

    Holds data we use in traversing bundle dependencies. Looks a lot like a dependency
    descriptor, but without an ID and version
    '''
    __slots__ = ()

    def __new__(cls, *args, excludes=(), **kwargs):
        return super(_BDTD, cls).__new__(cls, *args, excludes=excludes, **kwargs)

    def merge_excludes(self, excludes):
        return self._replace(excludes=self.excludes +
                tuple(e for e in excludes if e not in self.excludes))


class BundleContext(Context):
    '''
    `Context` for a bundle.
    '''


class _RemoteHandlerMixin(object):
    '''
    Utility mixin for handling remotes

    The mixed-in class must have a `remotes` attribute which is a list of `Remote`
    '''

    def _get_remotes(self, remotes):
        ''''
        Get remotes

        Parameters
        ----------
        remotes : iterable of Remote or str
            A subset of names of remotes to act on and additional remotes to act on
        '''

        instance_remotes = []
        additional_remotes = []
        if remotes:
            configured_remotes = {r.name: r for r in self.remotes}
            for r in remotes:
                if isinstance(r, six.text_type):
                    instance_remotes.append(configured_remotes.get(r))
                elif isinstance(r, Remote):
                    additional_remotes.append(r)
        else:
            instance_remotes = self.remotes
        has_remote = False
        for rem in chain(additional_remotes, instance_remotes):
            has_remote = True
            yield rem

        if not has_remote:
            raise NoRemoteAvailable()


class Fetcher(_RemoteHandlerMixin):
    '''
    Fetches bundles from `Remotes <Remote>`

    A fetcher takes a list of remotes, a bundle ID, and, optionally, a version number and
    downloads the bundle to a local directory. `Deployer` is, functionally, the dual of
    this class.
    '''

    def __init__(self, bundles_root, remotes):
        '''
        Parameters
        ----------
        bundles_root : str
            The root directory of the bundle cache
        remotes : list of Remote or str
            List of pre-configured remotes used in calls to `fetch`
        '''
        self.bundles_root = bundles_root
        self.remotes = remotes

    def __call__(self, *args, **kwargs):
        '''
        Calls `fetch` with the given arguments
        '''
        return self.fetch(*args, **kwargs)

    def fetch(self, bundle_id, bundle_version=None, remotes=None):
        '''
        Retrieve a bundle by name from a remote and put it in the local bundle cache.

        The first remote that can retrieve the bundle will be tried. Each remote will be
        tried in succession until one downloads the bundle.

        Parameters
        ----------
        bundle_id : str
            The id of the bundle to retrieve
        bundle_version : int
            The version of the bundle to retrieve. optional
        remotes : iterable of Remote or str
            A subset of remotes and additional remotes to fetch from. If an entry in the
            iterable is a string, then it will be looked for amongst the remotes passed in
            initially.

        Returns
        -------
        str
            returns the directory where the bundle has been placed

        Raises
        ------
        NoBundleLoader
            Thrown when none of the loaders are able to download the bundle
        BundleAlreadyExists
            Thrown when the requested bundle is already in the cache
        '''
        if remotes:
            remotes = list(remotes)
        given_bundle_version = bundle_version
        loaders = self._get_bundle_loaders(bundle_id, given_bundle_version, remotes)
        loaders_list = list(loaders)

        if bundle_version is None:
            bundle_version = self._find_latest_remote_bundle_versions(bundle_id, loaders_list)

        bdir = fmt_bundle_directory(self.bundles_root, bundle_id, bundle_version)
        self._assert_target_is_empty(bdir)

        for loader in loaders_list:
            try:
                loader.base_directory = bdir
                loader(bundle_id, bundle_version)
                with open(p(bdir, BUNDLE_MANIFEST_FILE_NAME)) as mf:
                    manifest_data = json.load(mf)
                    for dd in manifest_data.get('dependencies', ()):
                        try:
                            find_bundle_directory(self.bundles_root, dd['id'], dd.get('version'))
                        except BundleNotFound:
                            self.fetch(dd['id'], dd.get('version'), remotes=remotes)
                return bdir
            except Exception:
                L.warning('Failed to load bundle %s with %s', bundle_id, loader, exc_info=True)
                shutil.rmtree(bdir)
        else:  # no break
            raise NoBundleLoader(bundle_id, given_bundle_version)

    def _find_latest_remote_bundle_versions(self, bundle_id, loaders_list):
        latest_bundle_version = 0
        for loader in loaders_list:
            versions = loader.bundle_versions(bundle_id)
            if not versions:
                L.warning('Loader %s does not have any versions of the bundle %s', loader, bundle_id)
                continue
            loader_latest_version = max(versions)
            if loader_latest_version > latest_bundle_version:
                latest_bundle_version = loader_latest_version
        if latest_bundle_version <= 0:
            raise BundleNotFound(bundle_id, 'No versions of the requested bundle found from any remotes')
        return latest_bundle_version

    def _assert_target_is_empty(self, bdir):
        target_empty = True
        try:
            for _ in scandir(bdir):
                target_empty = False
                break
        except FileNotFoundError:
            return
        if not target_empty:
            raise FetchTargetIsNotEmpty(bdir)

    def _get_bundle_loaders(self, bundle_id, bundle_version, remotes):
        for rem in self._get_remotes(remotes):
            for loader in rem.generate_loaders():
                if loader.can_load(bundle_id, bundle_version):
                    yield loader


class Deployer(_RemoteHandlerMixin):
    '''
    Deploys bundles to `Remotes <Remote>`.

    A deployer takes a bundle directory tree or bundle archive and uploads it to a remote.
    `Fetcher` is, functionally, the dual of this class.

    Deployer is responsible for selecting remotes and corresponding uploaders among a set
    of options. `Uploaders <Uploader>` are responsible for actually doing the upload.
    '''

    def __init__(self, remotes=()):
        self.remotes = remotes

    def __call__(self, *args, **kwargs):
        return self.deploy(*args, **kwargs)

    def deploy(self, bundle_path, remotes=None):
        '''
        Deploy a bundle

        Parameters
        ----------
        bundle_path : str
            Path to a bundle directory tree or archive
        remotes : iterable of Remote or str
            A subset of remotes to deploy to and additional remotes to deploy to

        Raises
        ------
        NoAcceptableUploaders
            Thrown when none of the selected uploaders could upload the bundle
        '''
        if not exists(bundle_path):
            raise NotABundlePath(bundle_path, 'the file does not exist')

        manifest_data = self._extract_manifest_data_from_bundle_path(bundle_path)
        validate_manifest(bundle_path, manifest_data)

        uploaded = False
        for uploader in self._get_bundle_uploaders(bundle_path, remotes=remotes):
            uploader(bundle_path)
            uploaded = True

        if not uploaded:
            raise NoAcceptableUploaders(bundle_path)

    def _extract_manifest_data_from_bundle_path(self, bundle_path):
        if isdir(bundle_path):
            return self._get_directory_manifest_data(bundle_path)
        elif isfile(bundle_path):
            return self._get_archive_manifest_data(bundle_path)
        else:
            raise NotABundlePath(bundle_path, 'path does not point to a file or directory')

    def _get_bundle_uploaders(self, bundle_directory, remotes=None):
        for rem in self._get_remotes(remotes):
            for uploader in rem.generate_uploaders():
                if uploader.can_upload(bundle_directory):
                    yield uploader

    def _get_directory_manifest_data(self, bundle_path):
        try:
            with open(p(bundle_path, BUNDLE_MANIFEST_FILE_NAME)) as mf:
                return json.load(mf)
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT: # FileNotFound
                raise NotABundlePath(bundle_path, 'no bundle manifest found')
            if e.errno == errno.EISDIR: # IsADirectoryError
                raise NotABundlePath(bundle_path, 'manifest is not a regular file')
            raise
        except json.decoder.JSONDecodeError:
            raise NotABundlePath(bundle_path, 'manifest is malformed: expected a'
                    ' JSON file')

    def _get_archive_manifest_data(self, bundle_path):
        with Unarchiver().to_tarfile(bundle_path) as tf:
            try:
                mf0 = tf.extractfile(BUNDLE_MANIFEST_FILE_NAME)
                if mf0 is None:
                    raise NotABundlePath(bundle_path, 'manifest is not a regular file')
                # Would like to pull the
                with mf0 as mf:
                    return json.load(mf)
            except KeyError:
                raise NotABundlePath(bundle_path, 'no bundle manifest found')
            except json.decoder.JSONDecodeError:
                raise NotABundlePath(bundle_path, 'manifest is malformed: expected a'
                        ' JSON file')


class Uploader(object):
    '''
    Uploads bundles to remotes
    '''

    @classmethod
    def can_upload_to(self, accessor_config):
        '''
        Returns True if this uploader can upload with the given accessor configuration

        Parameters
        ----------
        accessor_config : AccessorConfig
        '''
        return False

    def can_upload(self, bundle_path):
        '''
        Returns True if this uploader can upload this bundle

        Parameters
        ----------
        bundle_path : str
            The file path to the bundle to upload
        '''
        return False

    def upload(self, bundle_path):
        '''
        Upload a bundle

        Parameters
        ----------
        bundle_path : str
            The file path to the bundle to upload
        '''
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        return self.upload(*args, **kwargs)

    @classmethod
    def register(cls):
        UPLOADER_CLASSES.append(cls)


class Cache(object):
    '''
    Cache of bundles
    '''

    def __init__(self, bundles_directory):
        '''
        Parameters
        ----------
        bundles_directory : str
            The where bundles are stored
        '''
        self.bundles_directory = bundles_directory

    def list(self):
        '''
        Returns a generator of summary bundle info
        '''
        try:
            bundle_directories = scandir(self.bundles_directory)
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT:
                return
            raise

        for bundle_directory in bundle_directories:
            if not bundle_directory.is_dir():
                continue

            # Ignore deletes out from under us
            try:
                version_directories = scandir(bundle_directory.path)
            except (OSError, IOError) as e:
                if e.errno == errno.ENOENT:
                    continue
                raise

            def keyfunc(x):
                try:
                    return int(x.name)
                except ValueError:
                    return float('+inf')

            for version_directory in sorted(version_directories, key=keyfunc, reverse=True):
                if not version_directory.is_dir():
                    continue
                try:
                    manifest_fname = p(version_directory.path, BUNDLE_MANIFEST_FILE_NAME)
                    with open(manifest_fname) as mf:
                        try:
                            manifest_data = json.load(mf)
                            bd_id = urlunquote(bundle_directory.name)
                            bd_version = int(version_directory.name)
                            if (bd_id != manifest_data.get('id') or
                                    bd_version != manifest_data.get('version')):
                                L.warning('Bundle manifest at %s does not match bundle'
                                ' directory', manifest_fname)
                                continue
                            yield manifest_data
                        except json.decoder.JSONDecodeError:
                            L.warning("Bundle manifest at %s is malformed",
                                   manifest_fname)
                except (OSError, IOError) as e:
                    if e.errno != errno.ENOENT:
                        raise


def retrieve_remotes(remotes_dir):
    '''
    Retrieve remotes from a owmeta_core project directory

    Parameters
    ----------
    owmdir : str
        path to the project directory
    '''
    if not exists(remotes_dir):
        return
    for r in listdir(remotes_dir):
        if r.endswith('.remote'):
            with open(p(remotes_dir, r)) as inp:
                try:
                    yield Remote.read(inp)
                except Exception:
                    L.warning('Unable to read remote %s', r, exc_info=True)


class Loader(object):
    '''
    Downloads bundles into the local index and caches them

    Attributes
    ----------
    base_directory : str
        The path where the bundle archive should be unpacked
    '''

    def __init__(self):
        # The base directory
        self.base_directory = None

    @classmethod
    def can_load_from(cls, accessor_config):
        '''
        Returns `True` if the given `accessor_config` is a valid config for this loader

        Parameters
        ----------
        accessor_config : AccessorConfig
            The config which we may be able to load from
        '''
        return False

    def can_load(self, bundle_id, bundle_version=None):
        '''
        Returns True if the bundle named `bundle_id` is available.

        This method is for loaders to determine that they probably can or cannot load the
        bundle, such as by checking repository metadata. Other loaders that return `True`
        from `can_load` should be tried if a given loader fails, but a warning should be
        recorded for the loader that failed.
        '''
        return False

    def bundle_versions(self, bundle_id):
        '''
        List the versions available for the bundle.

        This is a required part of the `Loader` interface.

        Parameters
        ----------
        bundle_id : str
            ID of the bundle for which versions are requested

        Returns
        -------
        A list of int
            Each entry is a version of the bundle available via this loader
        '''
        raise NotImplementedError()

    def load(self, bundle_id, bundle_version=None):
        '''
        Load the bundle into the local index

        Parameters
        ----------
        bundle_id : str
            ID of the bundle to load
        bundle_version : int
            Version of the bundle to load. Defaults to the latest available. optional
        '''
        raise NotImplementedError()

    def __call__(self, bundle_id, bundle_version=None):
        '''
        Load the bundle into the local index. Short-hand for `load`
        '''
        return self.load(bundle_id, bundle_version)

    @classmethod
    def register(cls):
        LOADER_CLASSES.append(cls)


class Installer(object):
    '''
    Installs a bundle locally
    '''

    def __init__(self, source_directory, bundles_directory, graph,
                 imports_ctx=None, default_ctx=None, installer_id=None, remotes=(), remotes_directory=None):
        '''
        Parameters
        ----------
        source_directory : str
            Directory where files come from. All files for a bundle must be below this
            directory
        bundles_directory : str
            Directory where the bundles files go. Usually this is the bundle cache
            directory
        graph : rdflib.graph.ConjunctiveGraph
            The graph from which we source contexts for this bundle
        default_ctx : str, optional
            The ID of the default context -- the target of a query when not otherwise
            specified.
        imports_ctx : str, optional
            The ID of the imports context this installer should use. Imports relationships
            are selected from this graph according to the included contexts.
        installer_id : iterable of Remote or str, optional
            Name of this installer for purposes of mutual exclusion
        remotes : iterable of Remote, optional
            Remotes to be used for retrieving dependencies when needed during
            installation. If not provided, the remotes will be collected from
            `remotes_directory`
        remotes_directory : str, optional
            The directory to load `Remotes <Remote>` from in case a bundle is not in the
            bundle cache. Defaults to `.DEFAULT_REMOTES_DIRECTORY`
        '''
        self.context_hash = hashlib.sha224
        self.file_hash = hashlib.sha224
        self.source_directory = source_directory
        self.bundles_directory = bundles_directory
        self.graph = graph
        self.installer_id = installer_id
        self.imports_ctx = imports_ctx
        self.default_ctx = default_ctx
        self.remotes = list(remotes)
        self.remotes_directory = remotes_directory

    def install(self, descriptor, progress_reporter=None):
        '''
        Given a descriptor, install a bundle

        Parameters
        ----------
        descriptor : Descriptor
            The descriptor for the bundle
        progress_reporter : `tqdm.tqdm <https://tqdm.github.io/>`_-like object
            Used for reporting progress during installation. optional

        Returns
        -------
        str
            The directory where the bundle is installed

        Raises
        ------
        TargetIsNotEmpty
            Thrown when the target directory for installation is not empty.
        '''
        # Create the staging directory in the base directory to reduce the chance of
        # moving across file systems
        try:
            staging_directory = fmt_bundle_directory(self.bundles_directory, descriptor.id,
                    descriptor.version)
            makedirs(staging_directory)
        except OSError:
            pass

        target_empty = True
        for _ in scandir(staging_directory):
            target_empty = False
            break
        if not target_empty:
            raise TargetIsNotEmpty(staging_directory)

        with lock_file(p(staging_directory, '.lock'), unique_key=self.installer_id):
            try:
                self._install(descriptor, staging_directory,
                        progress_reporter=progress_reporter)
                return staging_directory
            except Exception:
                self._cleanup_failed_install(staging_directory)
                raise

    def _cleanup_failed_install(self, staging_directory):
        shutil.rmtree(p(staging_directory, 'graphs'))
        shutil.rmtree(p(staging_directory, 'files'))

    def _install(self, descriptor, staging_directory, progress_reporter=None):
        graphs_directory, files_directory = self._set_up_directories(staging_directory)
        self._write_file_hashes(descriptor, files_directory)
        self._write_context_data(descriptor, graphs_directory)
        self._write_manifest(descriptor, staging_directory)
        self._build_indexed_database(staging_directory,
                progress_reporter=progress_reporter)

    def _set_up_directories(self, staging_directory):
        graphs_directory = p(staging_directory, 'graphs')
        files_directory = p(staging_directory, 'files')

        try:
            makedirs(graphs_directory)
            makedirs(files_directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        return graphs_directory, files_directory

    def _write_file_hashes(self, descriptor, files_directory):
        with open(p(files_directory, 'hashes'), 'wb') as hash_out:
            for fname in _select_files(descriptor, self.source_directory):
                hsh = self.file_hash()
                source_fname = p(self.source_directory, fname)
                with open(source_fname, 'rb') as fh:
                    hash_file(hsh, fh)
                hash_out.write(fname.encode('UTF-8') + b'\x00' + pack('B', hsh.digest_size) + hsh.digest() + b'\n')
                shutil.copy2(source_fname, p(files_directory, fname))

    def _write_context_data(self, descriptor, graphs_directory):
        contexts = _select_contexts(descriptor, self.graph)
        imports_ctxg = None
        if self.imports_ctx:
            imports_ctxg = self.graph.get_context(self.imports_ctx)

        included_context_ids = set()

        with open(p(graphs_directory, 'hashes'), 'wb') as hash_out,\
                open(p(graphs_directory, 'index'), 'wb') as index_out:
            imported_contexts = set()
            for ctxid, ctxgraph in contexts:
                hsh = self.context_hash()
                temp_fname = p(graphs_directory, 'graph.tmp')
                write_canonical_to_file(ctxgraph, temp_fname)
                with open(temp_fname, 'rb') as ctx_fh:
                    hash_file(hsh, ctx_fh)
                included_context_ids.add(ctxid)
                ctxidb = ctxid.encode('UTF-8')
                # Write hash
                hash_out.write(ctxidb + b'\x00' + pack('B', hsh.digest_size) + hsh.digest() + b'\n')
                gbname = hsh.hexdigest() + '.nt'
                # Write index
                index_out.write(ctxidb + b'\x00' + gbname.encode('UTF-8') + b'\n')

                ctx_file_name = p(graphs_directory, gbname)
                rename(temp_fname, ctx_file_name)

                if imports_ctxg is not None:
                    imported_contexts |= transitive_lookup(imports_ctxg,
                                                           ctxid,
                                                           CONTEXT_IMPORTS,
                                                           seen=imported_contexts)
            uncovered_contexts = imported_contexts - included_context_ids
            uncovered_contexts = self._cover_with_dependencies(uncovered_contexts, descriptor)
            if uncovered_contexts:
                raise UncoveredImports(uncovered_contexts)
            hash_out.flush()
            index_out.flush()

    def _write_manifest(self, descriptor, staging_directory):
        manifest_data = {}
        if self.default_ctx:
            manifest_data[DEFAULT_CONTEXT_KEY] = self.default_ctx
        if self.imports_ctx:
            # If an imports context was specified, then we'll need to generate an
            # imports context with the appropriate imports. We don't use the source
            # imports context ID for the bundle's imports context because the bundle
            # imports that we actually need are a subset of the total set of imports
            manifest_data[IMPORTS_CONTEXT_KEY] = fmt_bundle_ctx_id(descriptor.id)
        manifest_data['id'] = descriptor.id
        manifest_data['version'] = descriptor.version
        manifest_data['manifest_version'] = BUNDLE_MANIFEST_VERSION
        manifest_data['dependencies'] = [{'version': x.version, 'id': x.id, 'excludes': x.excludes}
                for x in descriptor.dependencies]
        self.manifest_data = manifest_data
        with open(p(staging_directory, BUNDLE_MANIFEST_FILE_NAME), 'w') as mf:
            json.dump(manifest_data, mf, separators=(',', ':'))

    def _initdb(self, staging_directory):
        self.conf = Data().copy({
            'rdf.source': 'default',
            'rdf.store': 'FileStorageZODB',
            'rdf.store_conf': p(staging_directory, BUNDLE_INDEXED_DB_NAME)
        })
        # Create the database file and initialize some needed data structures
        self.conf.init()
        if not exists(self.conf['rdf.store_conf']):
            raise Exception('Could not create the database file at ' + self.conf['rdf.store_conf'])

    def _build_indexed_database(self, staging_directory, progress_reporter=None):
        self._initdb(staging_directory)
        try:
            graphs_directory = p(staging_directory, 'graphs')
            idx_fname = p(graphs_directory, 'index')
            progress = progress_reporter
            if not exists(idx_fname):
                raise Exception('Cannot find an index at {}'.format(repr(idx_fname)))
            dest = self.conf['rdf.graph']
            if progress is not None:
                cnt = 0
                for l in dest.contexts():
                    cnt += 1
                progress.total = cnt

            with transaction.manager:
                with open(idx_fname, 'rb') as idx_file:
                    for line in idx_file:
                        line = line.strip()
                        if not line:
                            continue
                        ctx, _ = line.split(b'\x00')
                        ctxg = self.graph.get_context(ctx.decode('UTF-8'))
                        dest.addN(trip + (ctxg,) for trip in ctxg)

                        if progress is not None:
                            progress.update(1)
                if progress is not None:
                    progress.write('Finalizing writes to database...')
            if progress is not None:
                progress.write('Wrote indexed database')
        finally:
            if self.conf:
                self.conf.close()

    def _cover_with_dependencies(self, uncovered_contexts, descriptor):
        # XXX: Will also need to check for the contexts having a given ID being consistent
        # with each other across dependencies
        dependencies = descriptor.dependencies
        for d in dependencies:
            bnd = Bundle(d.id, self.bundles_directory, d.version, remotes=self.remotes,
                    remotes_directory=self.remotes_directory)
            for c in bnd.contexts:
                uncovered_contexts.discard(URIRef(c))
                if not uncovered_contexts:
                    break
        for c in descriptor.empties:
            uncovered_contexts.discard(URIRef(c))
            if not uncovered_contexts:
                break
        return uncovered_contexts


def fmt_bundle_ctx_id(id):
    return 'http://openworm.org/data/generated_imports_ctx?bundle_id=' + urlquote(id)


class FilesDescriptor(object):
    '''
    Descriptor for files
    '''
    def __init__(self):
        self.patterns = set()
        self.includes = set()

    @classmethod
    def make(cls, obj):
        if not obj:
            return
        res = cls()
        res.patterns = set(obj.get('patterns', ()))
        res.includes = set(obj.get('includes', ()))
        return res


def make_pattern(s):
    if s.startswith('rgx:'):
        return RegexURIPattern(s[4:])
    else:
        return GlobURIPattern(s)


def make_include_func(s):
    if isinstance(s, str):
        return URIIncludeFunc(s)
    elif isinstance(s, dict):
        uri = None
        for k in s.keys():
            if uri is not None:
                raise ValueError('Context "includes" entry must have one key--the URI of'
                        f' the context to include. Extra key is "{k}"')
            uri = k

        return URIIncludeFunc(uri)
    else:
        raise ValueError('Context "includes" entry must be a str or a dict')


class URIIncludeFunc(object):

    def __init__(self, include):
        self.include = URIRef(include.strip())

    def __hash__(self):
        return hash(self.include)

    def __call__(self, uri):
        return URIRef(uri.strip()) == self.include

    def __str__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.include))

    __repr__ = __str__


class URIPattern(object):
    def __init__(self, pattern):
        self._pattern = pattern

    def __hash__(self):
        return hash(self._pattern)

    def __call__(self, uri):
        return False

    def __str__(self):
        return '{}({})'.format(FCN(type(self)), self._pattern)


class RegexURIPattern(URIPattern):
    def __init__(self, pattern):
        super(RegexURIPattern, self).__init__(re.compile(pattern))

    def __call__(self, uri):
        # Cast the pattern match result to a boolean
        return not not self._pattern.match(str(uri))


class GlobURIPattern(RegexURIPattern):
    def __init__(self, pattern):
        replacements = [
            ['*', '.*'],
            ['?', '.?'],
            ['[!', '[^']
        ]

        for a, b in replacements:
            pattern = pattern.replace(a, b)
        super(GlobURIPattern, self).__init__(re.compile(pattern))


def _select_files(descriptor, directory):
    fdescr = descriptor.files
    if not fdescr:
        return
    for f in fdescr.includes:
        if not exists(p(directory, f)):
            raise Exception('Included file in bundle does not exist', f)
        yield f

    for f in fdescr.patterns:
        for match in match_files(directory, p(directory, f)):
            yield relpath(match, directory)


def _select_contexts(descriptor, graph):
    for context in graph.contexts():
        ctx = context.identifier
        for inc in descriptor.includes:
            if inc(ctx):
                yield ctx, context
                break

        for pat in descriptor.patterns:
            if pat(ctx):
                yield ctx, context
                break


LOADER_CLASSES = []


UPLOADER_CLASSES = []
