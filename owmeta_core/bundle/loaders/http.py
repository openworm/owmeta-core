import http.client
import io
import logging
from os.path import join as p
import re
import ssl
from urllib.parse import quote as urlquote, urlparse
import hashlib

from ...command_util import GenericUserError
from ...utils import FCN, getattrs

from .. import URLConfig
from ..archive import ensure_archive, Unarchiver
from ..common import BUNDLE_ARCHIVE_MIME_TYPE

from . import LoadFailed, Loader, Uploader


L = logging.getLogger(__name__)

PROVIDER_PATH_FORMAT = r'''
(?P<module>(?:\w+)(?:\.\w+)*)
:
(?P<provider>(?:\w+)(?:\.\w+)*)'''

PROVIDER_PATH_RE = re.compile(PROVIDER_PATH_FORMAT, flags=re.VERBOSE)


class HTTPSURLConfig(URLConfig):
    def __init__(self, *args, ssl_context_provider=None,
            ssl_context=None, **kwargs):
        super(HTTPSURLConfig, self).__init__(*args, **kwargs)
        self.ssl_context_provider = ssl_context_provider
        self._ssl_context = ssl_context

    def init_ssl_context(self):
        import importlib as IM
        if self._ssl_context is not None:
            return

        if self.ssl_context_provider:
            md = PROVIDER_PATH_RE.match(self.ssl_context_provider)
            if not md:
                raise HTTPSURLError('Format of the provider path is incorrect')
            module = md.group('module')
            provider = md.group('provider')
            m = IM.import_module(module)
            attr_chain = provider.split('.')
            ssl_context_provider = self._lookup_ssl_context_provider(m, attr_chain)

            try:
                ssl_context = ssl_context_provider()
            except Exception as e:
                raise HTTPSURLError('Error from SSL context provider'
                        f' "{self.ssl_context_provider}": {e}')

            if not isinstance(ssl_context, ssl.SSLContext):
                raise HTTPSURLError('Provider returned something other than an'
                        f' ssl.SSLContext: {ssl_context}')

            self._ssl_context = ssl_context

    def _lookup_ssl_context_provider(self, m, attr_chain):
        try:
            return getattrs(m, attr_chain)
        except AttributeError:
            raise HTTPSURLError(f'"{self.ssl_context_provider}" does not point to an'
                    ' SSL context provider')

    @property
    def ssl_context(self):
        self.init_ssl_context()
        return self._ssl_context

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_ssl_context']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._ssl_context = None

    def __str__(self):
        if self.ssl_context_provider:
            ssl_context_maybe = f'\n    SSL Context Provider: {self.ssl_context_provider}'
        else:
            ssl_context_maybe = ''
        return f'{self.url}{ssl_context_maybe}'


HTTPSURLConfig.register('https')


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
        index_url : str or URLConfig
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
        elif isinstance(index_url, URLConfig):
            self.index_url = index_url.url
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
        self._index = None

    def __repr__(self):
        return '{}({})'.format(FCN(type(self)), repr(self.index_url))

    def _setup_index(self):
        import requests
        if self._index is None:
            response = requests.get(self.index_url)
            if response.status_code != 200:
                raise IndexLoadFailed(response)
            self._index = response.json()

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
        except IndexLoadFailed:
            L.warn('Failed to set up the index for %s', self,
                exc_info=L.isEnabledFor(logging.DEBUG))
            return False
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
        '''
        Loads a bundle by downloading an index file, looking up the bundle location, and
        then downloading the bundle
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

        response = requests.get(bundle_url, stream=True)
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


def https_remote(self, *, ssl_context_provider=None):
    '''
    Provide additional parameters for HTTPS remote accessors

    Parameters
    ----------
    ssl_context_provider : str
        Path to a callable that provides a `ssl.SSLContext`. The format is similar to that
        for setuptools entry points: ``path.to.module:path.to.provider.callable``.
        Notably, there's no name and "extras" are not supported. optional.
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

    return self._write_remote()


class IndexLoadFailed(Exception):
    '''
    Thrown when the HTTP bundle loader cannot get its index
    '''
    def __init__(self, response):
        self.response = response
