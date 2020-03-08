from contextlib import contextmanager
from itertools import chain
from os import makedirs, rename, scandir, listdir, walk
from os.path import join as p, exists, relpath, realpath, abspath, expanduser, isdir, isfile, dirname
from struct import pack
import errno
import hashlib
import http.client
import io
import json
import logging
import re
import shutil
import tarfile
import tempfile

from rdflib import plugin
from rdflib.parser import Parser, create_input_source
from rdflib.term import URIRef
import six
import transaction
import yaml

from .command_util import DEFAULT_OWM_DIR
from .context import DEFAULT_CONTEXT_KEY, IMPORTS_CONTEXT_KEY, Context
from .context_common import CONTEXT_IMPORTS
from .data import Data
from .file_match import match_files
from .file_lock import lock_file
from .graph_serialization import write_canonical_to_file, gen_ctx_fname
from .rdf_utils import transitive_lookup, BatchAddGraph
from .utils import FCN

try:
    from urllib.parse import quote as urlquote, unquote as urlunquote, urlparse
except ImportError:
    from urllib import quote as urlquote, unquote as urlunquote
    from urlparse import urlparse

L = logging.getLogger(__name__)


BUNDLE_MANIFEST_VERSION = 1
'''
Current version number of the bundle manifest. Written by `Installer` and anticipated by
`Deployer` and `Fetcher`.
'''

BUNDLE_ARCHIVE_MIME_TYPE = 'application/x-gtar'
'''
MIME type for bundle archive files
'''


BUNDLE_INDEXED_DB_NAME = 'owm.db'
'''
Base name of the indexed database that gets built in a bundle directory during
installation
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
        self.accessor_configs.append(accessor_config)

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


class DependencyDescriptor(object):
    __slots__ = ('id', 'version')

    def __new__(cls, id, version=None):
        res = super(DependencyDescriptor, cls).__new__(cls)
        res.id = id
        res.version = version
        return res

    def __eq__(self, other):
        return self.id == other.id and self.version == other.version

    def __hash__(self):
        return hash((self.id, self.version))

    def __repr__(self):
        return '{}({}{})'.format(
                FCN(type(self)),
                repr(self.id),
                (', ' + repr(self.version)) if self.version is not None else '')


class AccessorConfig(object):
    '''
    Configuration for accessing a remote. Loaders are added to a remote according to which
    accessors are avaialble
    '''

    def __eq__(self, other):
        raise NotImplementedError()

    def __hash__(self):
        raise NotImplementedError()


class URLConfig(AccessorConfig):
    '''
    Configuration for accessing a remote with just a URL.
    '''

    def __init__(self, url):
        self.url = url

    def __eq__(self, other):
        return self.url == other.url

    def __hash__(self):
        return hash(self.url)

    def __str__(self):
        return '{}(url={})'.format(FCN(type(self)), repr(self.url))

    __repr__ = __str__


class HTTPSURLConfig(URLConfig):
    def __init__(self, *args, ssl_context=None, **kwargs):
        super(HTTPSURLConfig, self).__init__(*args, **kwargs)
        self.ssl_context = ssl_context


class Descriptor(object):
    '''
    Descriptor for a bundle
    '''
    def __init__(self, ident, **kwargs):
        self.id = ident
        self._set(kwargs)

    @classmethod
    def make(cls, obj):
        '''
        Makes a descriptor from the given object.
        '''
        res = cls(ident=obj['id'])
        res._set(obj)
        return res

    @classmethod
    def load(cls, descriptor_source):
        '''
        Load a descriptor

        Parameters
        ----------
        descriptor_source : str
            The descriptor source

        Raises
        ------
        NotADescriptor : Thrown when the object loaded from `descriptor_source` isn't a
        `dict`
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

        deps = set()
        for x in obj.get('dependencies', ()):
            if isinstance(x, six.string_types):
                deps.add(DependencyDescriptor(x))
            elif isinstance(x, dict):
                deps.add(DependencyDescriptor(**x))
            else:
                deps.add(DependencyDescriptor(*x))
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

    def __init__(self, ident, bundles_directory=None, version=None, conf=None, remotes=()):
        if not ident:
            raise Exception('ident must be non-None')
        self.ident = ident
        if bundles_directory is None:
            bundles_directory = expanduser(p('~', '.owmeta', 'bundles'))
        self.bundles_directory = bundles_directory
        if not conf:
            conf = {}

        conf.update({'rdf.source': 'default'})
        self.version = version
        self.remotes = remotes
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
            remotes = None
            if self.remotes:
                remotes = self.remotes

            if not remotes and exists(DEFAULT_OWM_DIR):
                # TODO: Make this search upwards in case the directory exists at a parent
                remotes = retrieve_remotes(DEFAULT_OWM_DIR)

            if remotes:
                f = Fetcher(self.bundles_directory, remotes)
                bundle_directory = f(self.ident, self.version)
            else:
                raise
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
            with open(p(bundle_directory, 'manifest')) as mf:
                manifest_data = json.load(mf)
                self.conf[DEFAULT_CONTEXT_KEY] = manifest_data.get(DEFAULT_CONTEXT_KEY)
                self.conf[IMPORTS_CONTEXT_KEY] = manifest_data.get(IMPORTS_CONTEXT_KEY)
            self.conf['rdf.store'] = 'agg'
            self.conf['rdf.store_conf'] = [('FileStorageZODB', p(bundle_directory,
                BUNDLE_INDEXED_DB_NAME))] + [
                ('FileStorageZODB', p(
                    find_bundle_directory(self.bundles_directory, dd['id'], dd.get('version')),
                    BUNDLE_INDEXED_DB_NAME))
                for dd in manifest_data.get('dependencies')]
            # Create the database file and initialize some needed data structures
            self.conf.init()

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

    def __call__(self, target):
        if target and hasattr(target, 'contextualize'):
            ctx = Context(self.conf.get(DEFAULT_CONTEXT_KEY, None), conf=self.conf)
            return target.contextualize(ctx.stored)
        return None


class BundleContext(Context):
    '''
    Marker for the default `Context` for a bundle
    '''


def validate_manifest(bundle_path, manifest_data):
    '''
    Validate manifest data in a `dict`

    Parameters
    ----------
    bundle_path : str
        The path to the bundle directory or archive. Used in the exception message if the
        manifest data is invalid
    manifest_data : dict
        The data from a manifest file

    Raises
    ------
    NotABundlePath
        Thrown in one of these conditions:

        - `manifest_data` lacks a `manifest_version`
        - `manifest_data` has a `manifest_version` > BUNDLE_MANIFEST_VERSION
        - `manifest_data` has a `manifest_version` <= 0
        - `manifest_data` lacks a `version`
        - `manifest_data` lacks an `id`
    '''
    manifest_version = manifest_data.get('manifest_version')
    if not manifest_version:
        raise NotABundlePath(bundle_path,
                'the bundle manifest has no manifest version')

    if manifest_version > BUNDLE_MANIFEST_VERSION or manifest_version <= 0:
        raise NotABundlePath(bundle_path,
                'the bundle manifest has an invalid manifest version')

    version = manifest_data.get('version')
    if not version:
        raise NotABundlePath(bundle_path,
                'the bundle manifest has no bundle version')

    ident = manifest_data.get('id')
    if not ident:
        raise NotABundlePath(bundle_path,
                'the bundle manifest has no bundle id')


def find_bundle_directory(bundles_directory, ident, version=None):
    # - look up the bundle in the bundle cache
    # - generate a config based on the current config load the config
    # - make a database from the graphs, if necessary (similar to `owm regendb`). If
    #   delete the existing database if it doesn't match the store config
    if version is None:
        bundle_root = fmt_bundle_directory(bundles_directory, ident)
        latest_version = 0
        try:
            ents = scandir(bundle_root)
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT: # FileNotFound
                raise BundleNotFound(ident, 'Bundle directory does not exist')
            raise

        for ent in ents:
            if ent.is_dir():
                try:
                    vn = int(ent.name)
                except ValueError:
                    # We may put things other than versioned bundle directories in
                    # this directory later, in which case this is OK
                    pass
                else:
                    if vn > latest_version:
                        latest_version = vn
        version = latest_version
    if not version:
        raise BundleNotFound(ident, 'No versioned bundle directories exist')
    res = fmt_bundle_directory(bundles_directory, ident, version)
    if not exists(res):
        if version is None:
            raise BundleNotFound(ident, 'Bundle directory does not exist')
        else:
            raise BundleNotFound(ident, 'Bundle directory does not exist for the specified version', version)
    return res


def fmt_bundle_directory(bundles_directory, ident, version=None):
    '''
    Get the directory for the given bundle identifier and version

    Parameters
    ----------
    ident : str
        Bundle identifier
    version : int
        Version number. If not provided, returns the directory containing all of the
        versions
    '''
    base = p(bundles_directory, urlquote(ident, safe=''))
    if version is not None:
        return p(base, str(version))
    else:
        return base


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

    A fetcher takes a list of remotes and a bundle bundle directory tree or bundle archive and uploads it to a remote.
    `Fetcher` is, functionally, the dual of this class. The specific
    '''

    def __init__(self, bundles_root, remotes):
        self.bundles_root = bundles_root
        self.remotes = remotes

    def __call__(self, *args, **kwargs):
        '''
        Calls `fetch` with the given arguments
        '''
        return self.fetch(*args, **kwargs)

    def fetch(self, bundle_id, bundle_version=None, remotes=None):
        '''
        Retrieve a bundle by name from a remote and put it in the local bundle index and
        cache.

        The first remote that can retrieve the bundle will be tried. Each bundle will be
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
        '''
        loaders = self._get_bundle_loaders(bundle_id, bundle_version, remotes)

        for loader in loaders:
            try:
                if bundle_version is None:
                    versions = loader.bundle_versions(bundle_id)
                    if not versions:
                        raise BundleNotFound(bundle_id, 'This loader does not have any'
                                ' versions of the bundle')
                    bundle_version = max(versions)
                bdir = fmt_bundle_directory(self.bundles_root, bundle_id, bundle_version)
                loader.base_directory = bdir
                loader(bundle_id, bundle_version)
                return bdir
            except Exception:
                L.warning("Failed to load bundle %s with %s", bundle_id, loader, exc_info=True)
        else:  # no break
            raise NoBundleLoader(bundle_id, bundle_version)

    def _get_bundle_loaders(self, bundle_id, bundle_version, remotes):
        for rem in self._get_remotes(remotes):
            for loader in rem.generate_loaders():
                if loader.can_load(bundle_id, bundle_version):
                    yield loader


class Deployer(_RemoteHandlerMixin):
    '''
    Deploys bundles to `Remotes <Remote>`.

    A deployer takes a bundle directory tree or bundle archive and uploads it to a remote.
    `Fetcher` is, functionally, the dual of this class. The specific
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
        '''
        if not exists(bundle_path):
            raise NotABundlePath(bundle_path, 'the file does not exist')

        if isdir(bundle_path):
            try:
                with open(p(bundle_path, 'manifest')) as mf:
                    manifest_data = json.load(mf)
            except (OSError, IOError) as e:
                if e.errno == errno.ENOENT: # FileNotFound
                    raise NotABundlePath(bundle_path, 'no bundle manifest found')
                if e.errno == errno.EISDIR: # IsADirectoryError
                    raise NotABundlePath(bundle_path, 'manifest is not a regular file')
                raise
            validate_manifest(bundle_path, manifest_data)
        elif isfile(bundle_path):
            # TODO: Handle bundle archives
            pass

        for uploader in self._get_bundle_uploaders(bundle_path, remotes=remotes):
            uploader(bundle_path)

    def _get_bundle_uploaders(self, bundle_directory, remotes=None):
        for rem in self._get_remotes(remotes):
            for uploader in rem.generate_uploaders():
                if uploader.can_upload(bundle_directory):
                    yield loader


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
                    manifest_fname = p(version_directory.path, 'manifest')
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


def retrieve_remotes(owmdir):
    '''
    Retrieve remotes from a owmeta_core project directory

    Parameters
    ----------
    owmdir : str
        path to the project directory
    '''
    remotes_dir = p(owmdir, 'remotes')
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
        ''' Returns True if the given accessor_config is a valid config for this loader '''
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


class HTTPBundleUploader(Uploader):
    def __init__(self, upload_url, ssl_context=None):
        '''
        Parameters
        ----------
        upload_url : str or URLConfig
            URL string or accessor config
        ssl_context : ssl.SSLContext
            SSL/TLS context to use for the connection. Overrides any context provided in
            `upload_url`
        '''
        super(HTTPBundleUploader, self).__init__()

        self.ssl_context = None

        if isinstance(upload_url, str):
            self.upload_url = upload_url
        elif isinstance(upload_url, HTTPSURLConfig):
            self.upload_url = upload_url.url
            self.ssl_context = upload_url.ssl_context
        elif isinstance(upload_url, URLConfig):
            self.upload_url = upload_url.url
        else:
            raise TypeError('Expecting a string or URLConfig. Received %s' %
                    type(upload_url))

        if ssl_context:
            self.ssl_context = ssl_context

    @classmethod
    def can_upload_to(self, accessor_config):
        return (isinstance(accessor_config, URLConfig) and
                (accessor_config.url.startswith('https://') or
                    accessor_config.url.startswith('http://')))

    def upload(self, bundle_path):
        archive_path = bundle_path

        with tempfile.TemporaryDirectory() as tempdir:
            if isdir(bundle_path):
                with tarfile.open(p(tempdir, 'bundle.tar.xz'), mode='w:xz') as ba:
                    archive_path = p(tempdir, 'bundle.tar.xz')
                    ba.add(bundle_path, arcname='.')
            if not tarfile.is_tarfile(archive_path):
                raise NotABundlePath(bundle_path, 'Expected a directory or a tar file')
            self._post(archive_path)

    def _post(self, archive):
        urlparse = six.moves.urllib.parse.urlparse
        parsed_url = urlparse(self.upload_url)
        if parsed_url.scheme == 'http':
            connection_ctor = http.client.HTTPConnection
        else:
            def connection_ctor(*args, **kwargs):
                return http.client.HTTPSConnection(*args,
                        context=self.ssl_context, **kwargs)
        conn = connection_ctor(parsed_url.netloc)
        with open(archive, 'rb') as f:
            conn.request("POST", "", body=f, headers={'Content-Type':
                BUNDLE_ARCHIVE_MIME_TYPE})
        response = conn.getresponse()


class HTTPBundleLoader(Loader):
    # TODO: Test this class...
    '''
    Loads bundles from HTTP(S) resources listed in an index file
    '''

    def __init__(self, index_url, cachedir=None, **kwargs):
        '''
        Parameters
        ----------
        index_url : str or URLConfig
            URL for the index file pointing to the bundle archives
        cachedir : str
            Directory where the index and any downloaded bundle archive should be cached.
            If provided, the index and bundle archive is cached in the given directory. If
            not provided, the index will be cached in memory and the bundle will not be
            cached.
        '''
        super(HTTPBundleLoader, self).__init__(**kwargs)

        if isinstance(index_url, str):
            self.index_url = index_url
        elif isinstance(index_url, URLConfig):
            self.index_url = index_url.url
        else:
            raise TypeError('Expecting a string or URLConfig. Received %s' %
                    type(index_url))

        self.cachedir = cachedir
        self._index = None

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.index_url))

    def _setup_index(self):
        import requests
        if self._index is None:
            response = requests.get(self.index_url)
            self._index = response.json()

    @classmethod
    def can_load_from(cls, ac):
        return (isinstance(ac, URLConfig) and
                (ac.url.startswith('https://') or
                    ac.url.startswith('http://')))

    def can_load(self, bundle_id, bundle_version=None):
        '''
        Check the index for an entry for the bundle.

        - If a version is given and the index has an entry for the bundle at that version
          and that entry gives a URL for the bundle, then we return `True`.

        - If no version is given and the index has an entry for the bundle at any version
          and that entry gives a URL for the bundle, then we return `True`.

        - Otherwise, we return `False`
        '''
        self._setup_index()
        binfo = self._index.get(bundle_id)
        if binfo:
            if bundle_version is None:
                for binfo_version, binfo_url in binfo.items():
                    try:
                        int(binfo_version)
                    except ValueError:
                        L.warning("Got unexpected non-version-number key '%s' in bundle index info", binfo_version)
                        continue
                    if self._bundle_url_is_ok(binfo_url):
                        return True
                return False
            if not isinstance(binfo, dict):
                return False

            binfo_url = binfo.get(str(bundle_version))
            return self._bundle_url_is_ok(binfo_url)

    def _bundle_url_is_ok(self, bundle_url):
        try:
            parsed_url = urlparse(bundle_url)
        except Exception:
            L.warning("Failed while parsing bundle URL", bundle_url)
            return False
        if parsed_url.scheme in ('http', 'https') and parsed_url.netloc:
            return True
        return False

    def bundle_versions(self, bundle_id):
        self._setup_index()
        binfo = self._index.get(bundle_id)

        if not binfo:
            return []

        res = []
        for k in binfo.keys():
            try:
                val = int(k)
            except ValueError:
                L.warning("Got unexpected non-version-number key '%s' in bundle index info", k)
            else:
                res.append(val)
        return res

    def load(self, bundle_id, bundle_version=None):
        '''
        Loads a bundle by downloading an index file
        '''
        import requests
        self._setup_index()
        binfo = self._index.get(bundle_id)
        if not binfo:
            raise LoadFailed(bundle_id, self, 'Bundle is not in the index')
        if not isinstance(binfo, dict):
            raise LoadFailed(bundle_id, self, 'Unexpected type of bundle info in the index')
        if bundle_version is None:
            max_vn = 0
            for k in binfo.keys():
                try:
                    val = int(k)
                except ValueError:
                    L.warning("Got unexpected non-version-number key '%s' in bundle index info", k)
                else:
                    if max_vn < val:
                        max_vn = val
            if not max_vn:
                raise LoadFailed(bundle_id, self, 'No releases found')
            bundle_version = max_vn
        bundle_url = binfo.get(str(bundle_version))
        if not self._bundle_url_is_ok(bundle_url):
            raise LoadFailed(bundle_id, self, 'Did not find a valid URL for "%s" at'
                    ' version %s' % (bundle_id, bundle_version))
        response = requests.get(bundle_url, stream=True)
        if self.cachedir is not None:
            bfn = urlquote(bundle_id)
            with open(p(self.cachedir, bfn), 'w') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            with open(p(self.cachedir, bfn), 'r') as f:
                Unarchiver().unpack(f, self.base_directory)
        else:
            # XXX Does this work?
            bio = io.BytesIO()
            bio.write(response.raw.read())
            bio.seek(0)
            Unarchiver().unpack(bio, self.base_directory)


class Unarchiver(object):
    '''
    Unpacks an archive file (e.g., a `tar.gz`) of a bundle
    '''
    def __init__(self, bundles_directory=None):
        self.bundles_directory = bundles_directory

    def unpack(self, input_file, target_directory=None):
        '''
        Unpack the archive file

        If `target_directory` is provided, and `bundles_directory` is provided at
        initialization, then if the bundle manifest doesn't match the expected archive
        path, then an exception is raised.

        Parameters
        ----------
        input_file : str or :term:`file object`
            The archive file
        target_directory : str
            The path where the archive should be unpacked. If this argument is not
            provided, then the target directory is derived from `bundles_directory` (see
            `fmt_bundle_directory`). optional

        Raises
        ------
        NotABundlePath
            Thrown in one of these conditions:

            - If the `input_file` is not an expected format (lzma-zipped TAR file)
            - If the `input_file` does not have a "manifest" file
            - If the `input_file` manifest file is invalid or is not a regular file (see
              `validate_manifest` for further details)
            - If the `input_file` is a file path and the corresponding file is not found

        '''

        # - If we were given a target directory, just unpack there...no complications
        #
        # - If we weren't given a target directory, then we have to extract the manifest,
        # read the version and name, then create the target directory
        if not self.bundles_directory and not target_directory:
            # TODO: Devise a better exception here
            raise UnarchiveFailed('Neither a bundles_directory nor a target_directory was'
                    ' provided. Cannot determine where to extract %s archive to.' %
                    input_file)
        try:
            self._unpack(input_file, target_directory)
        except tarfile.ReadError:
            raise NotABundlePath(self._bundle_file_name(input_file),
                'Unable to read archive file')

    def _bundle_file_name(self, input_file):
        '''
        Try to extract the bundle file name from `input_file`
        '''
        if isinstance(input_file, str):
            file_name = input_file
        elif hasattr(input_file, 'name'):
            file_name = input_file.name
        else:
            file_name = 'bundle archive file'

        return file_name

    def _unpack(self, input_file, target_directory):
        with self._to_tarfile(input_file) as ba:
            expected_target_directory = self._process_manifest(input_file, ba)

            if (target_directory and expected_target_directory and
                    expected_target_directory != target_directory):
                raise TargetDirectoryMismatch(target_directory, expected_target_directory)
            elif not target_directory:
                target_directory = expected_target_directory

            L.debug('extracting %s to %s', input_file, target_directory)
            target_directory_empty = True
            try:
                for _ in scandir(target_directory):
                    target_directory_empty = False
                    break
            except FileNotFoundError:
                pass
            if not target_directory_empty:
                raise UnarchiveFailed('Target directory, "%s", is not empty' %
                        target_directory)
            try:
                ArchiveExtractor(target_directory, ba).extract()
            except _BadArchiveFilePath:
                shutil.rmtree(target_directory)
                file_name = self._bundle_file_name(input_file)
                raise NotABundlePath(file_name, 'Archive contains files that point'
                    ' outside of the target directory')

    def _process_manifest(self, input_file, ba):
        try:
            ef = ba.extractfile('./manifest')
        except KeyError:
            file_name = self._bundle_file_name(input_file)
            raise NotABundlePath(file_name, 'archive has no manifest')

        with ef as manifest:
            file_name = self._bundle_file_name(input_file)
            if manifest is None:
                raise NotABundlePath(file_name, 'archive manifest is not a regular file')
            manifest_data = json.load(manifest)
            validate_manifest(file_name, manifest_data)
            bundle_id = manifest_data['id']
            bundle_version = manifest_data['version']
            if self.bundles_directory:
                return fmt_bundle_directory(self.bundles_directory, bundle_id, bundle_version)

    def __call__(self, *args, **kwargs):
        '''
        Unpack the archive file
        '''
        return self.unpack(*args, **kwargs)

    @contextmanager
    def _to_tarfile(self, input_file):
        if isinstance(input_file, str):
            try:
                archive_file = open(input_file, 'rb')
            except FileNotFoundError:
                file_name = self._bundle_file_name(input_file)
                raise NotABundlePath(input_file, 'file not found')

            with archive_file as f, self._to_tarfile0(f) as ba:
                yield ba
        else:
            if hasattr(input_file, 'read'):
                with self._to_tarfile0(input_file) as ba:
                    yield ba

    @contextmanager
    def _to_tarfile0(self, f):
        with tarfile.open(mode='r:xz', fileobj=f) as ba:
            yield ba


class ArchiveExtractor(object):
    def __init__(self, targetdir, tarfile):
        self._targetdir = targetdir
        self._tarfile = tarfile

    def extract(self):
        self._tarfile.extractall(self._targetdir, members=self._safemembers())

    def _realpath(self, path):
        return realpath(abspath(path))

    def _badpath(self, path, base=None):
        # joinpath will ignore base if path is absolute
        if base is None:
            base = self._targetdir
        return not self._realpath(p(self._targetdir, path)).startswith(base)

    def _badlink(self, info):
        # Links are interpreted relative to the directory containing the link
        # TODO: Test this
        tip = self._realpath(p(self._targetdir, dirname(info.name)))
        return self._badpath(info.linkname, base=tip)

    def _safemembers(self):
        for finfo in self._tarfile.members:
            if self._badpath(finfo.name):
                raise _BadArchiveFilePath(finfo.name, 'Path is outside of base path "%s"' % self._targetdir)
            elif finfo.issym() and self._badlink(finfo):
                raise _BadArchiveFilePath(finfo.name,
                        'Hard link points to "%s", outside of base path "%s"' % (finfo.linkname, self._targetdir))
            elif finfo.islnk() and self._badlink(finfo):
                raise _BadArchiveFilePath(finfo.name,
                        'Symlink points to "%s", outside of "%s"' % (finfo.linkname,
                            self._targetdir))
            else:
                yield finfo


class _BadArchiveFilePath(Exception):
    '''
    Thrown when an archive file path points outside of a given base directory
    '''
    def __init__(self, archive_file_path, error):
        super(_BadArchiveFilePath, self).__init__(
                'Disallowed archive file %s: %s' %
                (archive_file_path, error))
        self.archive_file_path = archive_file_path
        self.error = error


class Archiver(object):
    '''
    Archives a bundle directory tree
    '''
    def __init__(self, target_directory, bundles_directory):
        '''
        Parameters
        ----------
        target_directory : str
            Where to place archives
        bundles_directory : str
            Where the bundles are
        '''
        self.target_directory = target_directory
        self.bundles_directory = bundles_directory

    def pack(self, bundle_id, version=None, target_file_name=None):
        '''
        Pack an installed bundle into an archive file

        Parameters
        ----------
        bundle_id : str
            ID of the bundle to pack
        version : int, optional
            Bundle version
        target_file_name : str, optional
            Name of the archive file. If not provided, the name will be 'bundle.tar.xz'
            and will placed in the `target_directory`. Relative paths are relative to
            `target_directory`
        Raises
        ------
        BundleNotFound
            Thrown when the bundle with the given ID cannot be found, or cannot be found
            at the demanded version
        ArchiveTargetPathDoesNotExist
            Thrown when the path to the desired target file does not exist
        '''
        if not target_file_name:
            target_file_name = 'bundle.tar.xz'

        target_path = p(self.target_directory, target_file_name)

        bnd_directory = find_bundle_directory(self.bundles_directory, bundle_id, version)
        accept = self._filter
        try:
            _tf = tarfile.open(target_path, mode='w:xz')
        except FileNotFoundError as e:
            if e.filename == target_path:
                raise ArchiveTargetPathDoesNotExist(target_path)
            raise
        else:
            with _tf as tf:
                for dirpath, dirs, files in walk(bnd_directory):
                    for f in files:
                        fpath = p(dirpath, f)
                        rpath = relpath(fpath, start=bnd_directory)
                        if accept(rpath, fpath):
                            tf.add(fpath, rpath)
        return target_path

    def _filter(self, path, fullpath):
        '''
        Filters out file names that are not to be included in a bundle
        '''
        if path.startswith(BUNDLE_INDEXED_DB_NAME):
            # Skip the indexed DB -- the format isn't really designed for sharability and
            # we can regenerate it anyway.
            return False
        return True


class Installer(object):
    '''
    Installs a bundle locally
    '''

    # TODO: Make source_directory optional -- not every bundle needs files
    def __init__(self, source_directory, bundles_directory, graph,
                 imports_ctx=None, default_ctx=None, installer_id=None, remotes=()):
        '''
        Parameters
        ----------
        source_directory : str
            Directory where files come from
        bundles_directory : str
            Directory where the bundles files go
        installer_id : str
            Name of this installer for purposes of mutual exclusion. optional
        graph : rdflib.graph.ConjunctiveGraph
            The graph from which we source contexts for this bundle
        default_ctx : str
            The ID of the default context -- the target of a query when not otherwise
            specified. optional
        imports_ctx : str
            The ID of the imports context this installer should use. Imports relationships
            are selected from this graph according to the included contexts. optional
        remotes : iterable of Remote
            Remotes to be used for retrieving dependencies when needed during
            installation. If not provided, the remotes will be collected from the
            owmeta_core project directory. optional
        '''
        self.context_hash = hashlib.sha224
        self.file_hash = hashlib.sha224
        self.source_directory = source_directory
        self.bundles_directory = bundles_directory
        self.graph = graph
        self.installer_id = installer_id
        self.imports_ctx = imports_ctx
        self.default_ctx = default_ctx
        self.remotes = remotes

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
            uncovered_contexts = self._cover_with_dependencies(uncovered_contexts, descriptor.dependencies)
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
        manifest_data['dependencies'] = [{'version': x.version, 'id': x.id} for x in descriptor.dependencies]
        self.manifest_data = manifest_data
        with open(p(staging_directory, 'manifest'), 'w') as mf:
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
            contexts = set()
            graphs_directory = p(staging_directory, 'graphs')
            idx_fname = p(graphs_directory, 'index')
            progress = progress_reporter
            if not exists(idx_fname):
                raise Exception('Cannot find an index at {}'.format(repr(idx_fname)))
            if progress is not None:
                cnt = 0
                for l in dest.contexts():
                    cnt += 1
                progress.total = cnt

            dest = self.conf['rdf.graph']
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
                self.conf.destroy()

    def _cover_with_dependencies(self, uncovered_contexts, dependencies):
        # XXX: Will also need to check for the contexts having a given ID being consistent
        # with each other across dependencies
        for d in dependencies:
            bnd = Bundle(d.id, self.bundles_directory, d.version, remotes=self.remotes)
            for b in bnd.contexts:
                uncovered_contexts.discard(URIRef(b))
                if not uncovered_contexts:
                    break
        return uncovered_contexts


def fmt_bundle_ctx_id(id):
    return 'http://openworm.org/data/generated_imports_ctx?bundle_id=' + urlquote(id)


def hash_file(hsh, fh, blocksize=None):
    '''
    Hash a file in chunks to avoid eating up too much memory at a time
    '''
    if not blocksize:
        blocksize = hsh.block_size << 15
    while True:
        block = fh.read(blocksize)
        if not block:
            break
        hsh.update(block)


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
    return URIIncludeFunc(s)


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


LOADER_CLASSES = [
    HTTPBundleLoader
]


UPLOADER_CLASSES = [
    HTTPBundleUploader
]


class BundleNotFound(Exception):
    def __init__(self, bundle_ident, msg=None, version=None):
        msg = 'Missing bundle "{}"{}{}'.format(bundle_ident,
                '' if version is None else ' at version ' + str(version),
                ': ' + str(msg) if msg is not None else '')
        super(BundleNotFound, self).__init__(msg)


class LoadFailed(Exception):
    def __init__(self, bundle, loader, *args):
        msg = args[0]
        mmsg = 'Failed to load {} bundle with loader {}{}'.format(
                bundle, loader, ': ' + msg if msg else '')
        super(LoadFailed, self).__init__(mmsg, *args[1:])


class InstallFailed(Exception):
    '''
    Thrown when a bundle installation fails to complete.

    You can assume that any intermediate bundle files have been cleaned up from the bundle
    cache
    '''


class UncoveredImports(InstallFailed):
    '''
    Thrown when a bundle to be installed is missing
    '''
    def __init__(self, imports):
        msg = 'Missing {} imports'.format(len(imports))
        super(UncoveredImports, self).__init__(msg)
        self.imports = imports


class TargetIsNotEmpty(InstallFailed):
    '''
    Thrown when the target directory of an installation is not empty
    '''
    def __init__(self, target):
        msg = 'Bundle installation target directory, "%s", is not empty' % target
        super(TargetIsNotEmpty, self).__init__(msg)
        self.directory = target


class FetchFailed(Exception):
    ''' Generic message for when a fetch fails '''
    pass


class NoBundleLoader(FetchFailed):
    '''
    Thrown when a loader can't be found for a bundle
    '''

    def __init__(self, bundle_id, bundle_version=None):
        super(NoBundleLoader, self).__init__(
            'No loader could be found for "%s"%s' % (bundle_id,
                (' at version ' + str(bundle_version)) if bundle_version is not None else ''))
        self.bundle_id = bundle_id
        self.bundle_version = bundle_version


class NotABundlePath(Exception):
    '''
    Thrown when a given path does not point to a valid bundle directory tree or bundle
    archive
    '''
    def __init__(self, path, explanation):
        message = '"{}" is not a bundle path: {}'.format(path, explanation)
        super(NotABundlePath, self).__init__(message)
        self.path = path


class NoRemoteAvailable(Exception):
    '''
    Thrown when we need a remote and we don't have one
    '''


class UnarchiveFailed(Exception):
    '''
    Thrown when an `Unarchiver` fails for some reason not covered by other
    '''


class ArchiveTargetPathDoesNotExist(Exception):
    '''
    Thrown when the `Archiver` target path does not exist
    '''


class NotADescriptor(Exception):
    '''
    Thrown when a given file, string, or other object is offered as a descriptor, but does
    not represent a `Descriptor`
    '''


class TargetDirectoryMismatch(UnarchiveFailed):
    '''
    Thrown when the target path doesn't agree with the bundle manifest
    '''
    def __init__(self, target_directory, expected_target_directory):
        super(TargetDirectoryMismatch, self).__init__(
                'Target directory "%s" does not match expected directory "%s" for the'
                ' bundle manifest.'
                % (target_directory, expected_target_directory))
        self.target_directory = target_directory
        self.expected_target_directory = expected_target_directory
