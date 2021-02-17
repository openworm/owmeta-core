import logging
from pathlib import Path
from urllib.parse import urlparse
import shutil

from . import LoadFailed, Loader
from .. import URLConfig
from ..common import find_bundle_directory, BundleNotFound, BundleTreeFileIgnorer


L = logging.getLogger(__name__)


class FileURLConfig(URLConfig):
    '''
    URL config for local files.

    Local file paths, in general, are not especially portable, but this accessor config
    may be useful for bundle directories on shared file systems like NFS or Samba.
    '''


FileURLConfig.register('file')


class FileBundleLoader(Loader):
    '''
    Copies bundles from a local directory structure identical to the local bundle cache
    typically stored under :file:`~/.owmeta/bundles`.

    Note, there is no corresponding bundle uploader: if you want that, you should instead
    `fetch <owmeta_core.bundle.Fetcher>` the bundle into the target bundle cache
    directory.
    '''
    def __init__(self, source_bundles_dir):
        if isinstance(source_bundles_dir, str):
            self.source_bundles_dir = Path(source_bundles_dir)
        elif isinstance(source_bundles_dir, Path):
            self.source_bundles_dir = source_bundles_dir
        elif isinstance(source_bundles_dir, FileURLConfig):
            self.source_bundles_dir = Path(urlparse(source_bundles_dir.url).path)
        elif isinstance(source_bundles_dir, URLConfig):
            parsed = urlparse(source_bundles_dir.url)
            if parsed.scheme == 'file':
                self.source_bundles_dir = Path(parsed.path)
            else:
                raise ValueError(f'Expected a file URL, but got {source_bundles_dir.url}')
        else:
            raise TypeError('FileBundleLoader config is invalid. Expected a str,'
                    ' Path-like object, or a FileURLConfig')

        if not self.source_bundles_dir.is_absolute():
            raise ValueError('Must use absolute path names for this loader. Given:'
                    f' {self.source_bundles_dir}')

    @classmethod
    def can_load_from(cls, ac):
        '''
        Returns `True` for ``file://`` `URLConfigs <URLConfig>`

        Parameters
        ----------
        ac : AccessorConfig
            The config which we may be able to load from
        '''
        return isinstance(ac, URLConfig) and ac.url.startswith('file://')

    def can_load(self, bundle_id, bundle_version=None):
        '''
        Check if the bundle is available under the base directory given at init
        '''
        try:
            find_bundle_directory(self.source_bundles_dir, bundle_id, bundle_version)
            return True
        except BundleNotFound:
            # This is a warn log because although there *may* be more than one loader that
            # can handle file:// URLs, I would rather this gets seen and has to be
            # supressed than that it's at debug or info and has to be sought out.
            L.warning('Unable to load bundle %s at version %d with %s',
                    bundle_id, bundle_version, self, exc_info=True)
            return False

    def load(self, bundle_id, bundle_version=None):
        try:
            src = find_bundle_directory(self.source_bundles_dir, bundle_id, bundle_version)
        except BundleNotFound as e:
            version_part = ''
            if bundle_version is not None:
                version_part = f' at version {bundle_version}'
            raise LoadFailed(bundle_id, self, 'Could not find source directory for the'
                    f' bundle{version_part}') from e

        ignorer = BundleTreeFileIgnorer(src)

        try:
            shutil.copytree(src, self.base_directory, ignore=ignorer)
        except Exception as e:
            version_part = ''
            if bundle_version is not None:
                version_part = f' at version {bundle_version}'
            raise LoadFailed(bundle_id, self, f'Failed to copy the bundle{version_part}'
                    ' from the source directory') from e
