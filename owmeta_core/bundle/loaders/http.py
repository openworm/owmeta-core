import http.client
import io
import logging
import os
from os.path import join as p, expanduser
import ssl
from urllib.parse import quote as urlquote, urlparse
import hashlib
import json
import pickle

from cachecontrol import CacheControl
from cachecontrol.caches.file_cache import FileCache
import requests

from ...command_util import GenericUserError
from ...utils import FCN, retrieve_provider

from .. import URLConfig
from ..archive import ensure_archive, Unarchiver
from ..common import BUNDLE_ARCHIVE_MIME_TYPE

from . import LoadFailed, Loader, Uploader


L = logging.getLogger(__name__)


class HTTPURLConfig(URLConfig):
    '''
    HTTP URL configuration
    '''

    def __init__(self, *args,
            session_file_name=None,
            session_provider=None,
            cache_dir=None,
            mem_cache=False,
            **kwargs):
        '''
        Parameters
        ----------
        *args
            Passed on to URLConfig
        session_file_name : str, optional
            Session file name
        session_provider : str, optional
            Provider path for a callable that returns a session
        cache_dir : str, optional
            HTTP cache directory. Supersedes `mem_cache`
        mem_cache : bool, optional
            Whether to use an in-memory cache. Superseded by `cache_dir`
        **kwargs
            Passed on to URLConfig
        '''
        super(HTTPURLConfig, self).__init__(*args, **kwargs)
        self.cache_dir = cache_dir
        self.session_file_name = session_file_name
        self.session_provider = session_provider
        self.mem_cache = bool(mem_cache)
        self._session = None

    @property
    def session(self):
        '''
        A `requests.Session`

        This will be loaded from `.session_file_name` if a value is set for that.
        Otherwise, the session will either be obtained from the `.session_provider` or a
        default session will be created; in either case, any response caching
        configuration will be applied.
        '''
        if self._session is None:
            if self.session_file_name:
                try:
                    with open(expanduser(self.session_file_name), 'rb') as session_file:
                        self._session = pickle.load(session_file)
                except FileNotFoundError:
                    pass

            if self._session is None:
                self.init_session()

        return self._session

    def init_session(self):
        '''
        Initialize the HTTP session. Typically you won't call this, but will just access
        `.session`
        '''
        self._session = self._make_new_session()

    def _make_new_session(self):
        if self.session_provider:
            session = self._provide_session()
        else:
            session = requests.Session()

        if self.cache_dir:
            http_cache = FileCache(self.cache_dir)
            return CacheControl(session, cache=http_cache)
        elif self.mem_cache:
            return CacheControl(session)
        else:
            return session

    def _provide_session(self):
        return retrieve_provider(self.session_provider)()

    def save_session(self):
        sfname = expanduser(self.session_file_name)
        with open(sfname + '.tmp', 'wb') as session_file:
            pickle.dump(self._session, session_file)
        os.rename(sfname + '.tmp', sfname)

    def __getstate__(self):
        state = self.__dict__.copy()
        # We're storing the session in a separate file (if at all), so obviously we don't
        # to persist it with the HTTPURLConfig
        del state['_session']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._session = None


class HTTPSURLConfig(HTTPURLConfig):
    '''
    HTTPS URL configuration
    '''

    def __init__(self, *args, ssl_context_provider=None,
            ssl_context=None, **kwargs):
        '''
        Parameters
        ----------
        *args
            Passed on to HTTPURLConfig
        ssl_context_provider : str
            Path to a callable that provides a `ssl.SSLContext`. See `https_remote`
        ssl_context : ssl.SSLContext
            The SSL/TLS context to use for uploading with this accessor
        **kwargs
            Passed on to HTTPURLConfig
        '''
        super(HTTPSURLConfig, self).__init__(*args, **kwargs)
        self.ssl_context_provider = ssl_context_provider
        self._ssl_context = ssl_context

    def init_ssl_context(self):
        if self._ssl_context is not None:
            return

        if self.ssl_context_provider:
            ssl_context_provider = self._lookup_ssl_context_provider()

            try:
                ssl_context = ssl_context_provider()
            except Exception as e:
                raise HTTPSURLError('Error from SSL context provider'
                        f' "{self.ssl_context_provider}": {e}')

            if not isinstance(ssl_context, ssl.SSLContext):
                raise HTTPSURLError('Provider returned something other than an'
                        f' ssl.SSLContext: {ssl_context}')

            self._ssl_context = ssl_context

    def _lookup_ssl_context_provider(self):
        try:
            return retrieve_provider(self.ssl_context_provider)
        except ValueError:
            raise HTTPSURLError('Format of the provider path is incorrect')
        except AttributeError:
            raise HTTPSURLError(f'"{self.ssl_context_provider}" does not point to an'
                    ' SSL context provider')

    @property
    def ssl_context(self):
        self.init_ssl_context()
        return self._ssl_context

    def __getstate__(self):
        state = super(HTTPSURLConfig, self).__getstate__()
        del state['_ssl_context']
        return state

    def __setstate__(self, state):
        super(HTTPSURLConfig, self).__setstate__(state)
        self._ssl_context = None

    def __str__(self):
        if self.ssl_context_provider:
            ssl_context_maybe = f'\n    SSL Context Provider: {self.ssl_context_provider}'
        else:
            ssl_context_maybe = ''
        return f'{self.url}{ssl_context_maybe}'


HTTPSURLConfig.register('https')
HTTPURLConfig.register('http')


class HTTPSURLError(Exception):
    pass


class HTTPBundleLoader(Loader):
    '''
    Loads bundles from HTTP(S) resources listed in an index file
    '''

    def __init__(self, index_url, cachedir=None, hash_preference=('sha224',), **kwargs):
        '''
        Parameters
        ----------
        index_url : str or owmeta_core.bundle.URLConfig
            URL for the index file pointing to the bundle archives
        cachedir : str, optional
            Directory where the index and any downloaded bundle archive should be cached.
            If provided, the index and bundle archive is cached in the given directory. If
            not provided, the index will be cached in memory and the bundle will not be
            cached.
        hash_preference : tuple of str
            Preference ordering of hashes to use for checking integrity of files. If none
            match in the preference ordering, then the first one
        **kwargs
            Passed on to `.Loader`
        '''
        super(HTTPBundleLoader, self).__init__(**kwargs)

        if isinstance(index_url, str):
            self.index_url = index_url
            self._url_config = None
        elif isinstance(index_url, HTTPURLConfig):
            self.index_url = index_url.url
            self._url_config = index_url
        elif isinstance(index_url, URLConfig):
            self.index_url = index_url.url
            self._url_config = None
        else:
            raise TypeError('Expecting a string or URLConfig. Received %s' %
                    type(index_url))

        if not hash_preference:
            hash_preference = tuple(hash_preference)

        for hash_name in hash_preference:
            if hash_name not in hashlib.algorithms_available:
                raise ValueError(f'"{hash_name}" is not available in hashlib on this system')

        self.hash_preference = hash_preference
        self.cachedir = cachedir
        self._session = getattr(index_url, 'session', None) or requests.Session()
        self._index = None

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.index_url))

    def _setup_index(self):
        if self._index is None:
            response = self._session.get(self.index_url)
            if response.status_code != 200:
                raise IndexLoadFailed(response)
            try:
                self._index = response.json()
            except json.decoder.JSONDecodeError:
                raise IndexLoadFailed(response)

    @classmethod
    def can_load_from(cls, ac):
        '''
        Returns `True` for ``http://`` or ``https://`` `URLConfigs <URLConfig>`

        Parameters
        ----------
        ac : AccessorConfig
            The config which we may be able to load from
        '''
        return (isinstance(ac, URLConfig) and
                (ac.url.startswith('https://') or
                    ac.url.startswith('http://')))

    def _save_session(self):
        if not self._url_config:
            return

        try:
            self._url_config.save_session()
        except Exception:
            L.warning('Error while attempting to save session', exc_info=True)

    def can_load(self, bundle_id, bundle_version=None):
        '''
        Check the index for an entry for the bundle.

        - If a version is given and the index has an entry for the bundle at that version
          and that entry gives a URL for the bundle, then we return `True`.

        - If no version is given and the index has an entry for the bundle at any version
          and that entry gives a URL for the bundle, then we return `True`.

        - Otherwise, we return `False`

        Parameters
        ----------
        bundle_id : str
            ID of the bundle to look for
        bundle_version : int, optional
            Version number of the bundle to look for. If not provided, then any version is
            deemed acceptable

        Returns
        -------
        bool
            `True` if the bundle can be loaded; otherwise, `False`
        '''
        try:
            self._setup_index()
            self._save_session()
        except IndexLoadFailed:
            L.warning('Failed to set up the index for %s', self,
                exc_info=L.isEnabledFor(logging.DEBUG))
            return False
        binfo = self._index.get(bundle_id)
        if binfo:
            if bundle_version is None:
                for binfo_version, versioned_binfo in binfo.items():
                    try:
                        int(binfo_version)
                    except ValueError:
                        L.warning("Got unexpected non-version-number key '%s' in bundle index info", binfo_version)
                        continue
                    try:
                        binfo_url = versioned_binfo.get('url')
                    except AttributeError:
                        L.warning("Got unexpected bundle info for version '%s' in bundle index info", binfo_version)
                        continue

                    if self._bundle_url_is_ok(binfo_url):
                        return True
                return False
            if not isinstance(binfo, dict):
                return False

            versioned_binfo = binfo.get(str(bundle_version))
            try:
                binfo_url = versioned_binfo.get('url')
            except AttributeError:
                L.warning("Got unexpected bundle info for version '%s' in bundle index info", versioned_binfo)
                return False
            return self._bundle_url_is_ok(binfo_url)

    def _bundle_url_is_ok(self, bundle_url):
        try:
            parsed_url = urlparse(bundle_url)
        except Exception:
            L.warning("Failed while parsing bundle URL '%s'", bundle_url)
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
        try:
            self._load(bundle_id, bundle_version)
        finally:
            self._save_session()

    def _load(self, bundle_id, bundle_version=None):
        '''
        Loads a bundle by downloading an index file, looking up the bundle location, and
        then downloading the bundle
        '''
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

        versioned_binfo = binfo.get(str(bundle_version))

        if not versioned_binfo or not isinstance(versioned_binfo, dict):
            raise LoadFailed(bundle_id, self, f'No bundle info for version {bundle_version}')

        bundle_url = versioned_binfo.get('url')

        if not self._bundle_url_is_ok(bundle_url):
            raise LoadFailed(bundle_id, self, 'Did not find a valid URL for "%s" at'
                    ' version %s' % (bundle_id, bundle_version))

        hashes = versioned_binfo.get('hashes')
        if not isinstance(hashes, dict) or not hashes:
            raise LoadFailed(bundle_id, self, f'No hash info for version {bundle_version}')

        for hash_name in self.hash_preference:
            bundle_hash = hashes.get(hash_name)
            if bundle_hash:
                break
        else: # no break
            for hash_name, bundle_hash in hashes.items():
                if hash_name in hashlib.algorithms_available:
                    break
            else: # no break
                raise LoadFailed(bundle_id, self, f'No supported hash for version {bundle_version}')

        try:
            hsh = hashlib.new(hash_name)
        except ValueError:
            L.warning('Hash in hashlib.algorithms_available unsupported in hashlib.new')
            raise LoadFailed(bundle_id, self, f'Unsupported hash {hash_name} for version {bundle_version}')

        response = self._session.get(bundle_url, stream=True)
        if self.cachedir is not None:
            bfn = urlquote(bundle_id)
            with open(p(self.cachedir, bfn), 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    hsh.update(chunk)
                    f.write(chunk)
            if bundle_hash != hsh.hexdigest():
                raise LoadFailed(bundle_id, self,
                        f'Failed to verify {hash_name} hash for version {bundle_version}')
            with open(p(self.cachedir, bfn), 'rb') as f:
                Unarchiver().unpack(f, self.base_directory)
        else:
            bio = io.BytesIO()
            bundle_bytes = response.raw.read()
            hsh.update(bundle_bytes)
            if bundle_hash != hsh.hexdigest():
                raise LoadFailed(bundle_id, self,
                        f'Failed to verify {hash_name} hash for version {bundle_version}')
            bio.write(bundle_bytes)
            bio.seek(0)
            Unarchiver().unpack(bio, self.base_directory)


class HTTPBundleUploader(Uploader):
    '''
    Uploads bundles by sending bundle archives in HTTP POST requests
    '''

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
        with ensure_archive(bundle_path) as archive_path:
            self._post(archive_path)

    def _post(self, archive):
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
        # XXX: Do something with this response
        # conn.getresponse()


def http_remote(self, *, cache=None, session_provider=None, session_file_name=None):
    '''
    Provide additional parameters for HTTP remote accessors

    Parameters
    ----------
    cache : str
        Either the string "mem" or a file path to a cache directory
    session_provider : str
        Path to a callable that provides a `requests.Session`. The format is similar to
        that for setuptools entry points: ``path.to.module:path.to.provider.callable``.
        Notably, there's no name and "extras" are not supported. optional.
    session_file_name : str
        Path to a file where the HTTP session can be stored
    '''

    if self._url_config is None:
        raise GenericUserError('An HTTP URL must be specified for HTTP accessors')

    if not isinstance(self._url_config, HTTPURLConfig):
        raise GenericUserError(f'The specified URL, {self._url_config} is not an HTTP URL')

    _http_urlconfig_command_helper(self, cache, session_provider, session_file_name)

    return self._write_remote()


def https_remote(self, *, ssl_context_provider=None, cache=None, session_provider=None,
        session_file_name=None):
    '''
    Provide additional parameters for HTTPS remote accessors

    Parameters
    ----------
    ssl_context_provider : str
        Path to a callable that provides a `ssl.SSLContext` used for bundle uploads. The
        format is similar to that for setuptools entry points:
        ``path.to.module:path.to.provider.callable``.  Notably, there's no name and
        "extras" are not supported. optional.
    cache : str
        Either the string "mem" or a file path to a cache directory
    session_provider : str
        Path to a callable that provides a `requests.Session`. The format is similar to
        that for setuptools entry points: ``path.to.module:path.to.provider.callable``.
        Notably, there's no name and "extras" are not supported. optional.
    session_file_name : str
        Path to a file where the HTTP session can be stored
    '''

    if self._url_config is None:
        raise GenericUserError('An HTTPS URL must be specified for HTTPS accessors')

    if not isinstance(self._url_config, HTTPSURLConfig):
        raise GenericUserError(f'The specified URL, {self._url_config} is not an HTTPS URL')

    try:
        if ssl_context_provider:
            self._url_config.ssl_context_provider = ssl_context_provider
            self._url_config.init_ssl_context()
    except HTTPSURLError as e:
        raise GenericUserError(str(e))

    _http_urlconfig_command_helper(self, cache, session_provider, session_file_name)

    return self._write_remote()


def _http_urlconfig_command_helper(self, cache, session_provider, session_file_name):
    if cache == 'mem':
        self._url_config.mem_cache = True
    else:
        self._url_config.cache_dir = cache

    if session_file_name:
        self._url_config.session_file_name = session_file_name

    if session_provider:
        self._url_config.session_provider = session_provider

    # Initialize a session to make sure the configs work.
    self._url_config._make_new_session()


class IndexLoadFailed(Exception):
    '''
    Thrown when the HTTP bundle loader cannot get its index
    '''
    def __init__(self, response):
        self.response = response
