from os import scandir
from os.path import join as p, exists
import errno

try:
    from urllib.parse import quote as urlquote
except ImportError:
    from urllib import quote as urlquote

from .exceptions import NotABundlePath, BundleNotFound


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

BUNDLE_MANIFEST_FILE_NAME = 'manifest'
'''
Name of the manifest file in a bundle directory or archive
'''


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
                raise BundleNotFound(ident, 'Bundle directory does not exist') from e
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
            raise BundleNotFound(ident,
                    f'Bundle directory, "{res}", does not exist')
        else:
            raise BundleNotFound(ident,
                    f'Bundle directory, "{res}", does not exist for the specified version', version)
    return res
