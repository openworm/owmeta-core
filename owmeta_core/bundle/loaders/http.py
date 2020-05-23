import http.client
import io
import logging
from urllib.parse import quote as urlquote, urlparse
from os.path import join as p

from ...utils import FCN
from .. import Loader, Uploader, URLConfig
from ..archive import ensure_archive, Unarchiver
from . import LoadFailed
from ..common import BUNDLE_ARCHIVE_MIME_TYPE


L = logging.getLogger(__name__)


class HTTPSURLConfig(URLConfig):
    def __init__(self, *args, ssl_context=None, **kwargs):
        super(HTTPSURLConfig, self).__init__(*args, **kwargs)
        self.ssl_context = ssl_context


class HTTPBundleLoader(Loader):
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
            with open(p(self.cachedir, bfn), 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            with open(p(self.cachedir, bfn), 'rb') as f:
                Unarchiver().unpack(f, self.base_directory)
        else:
            bio = io.BytesIO()
            bio.write(response.raw.read())
            bio.seek(0)
            Unarchiver().unpack(bio, self.base_directory)


HTTPBundleLoader.register()


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
