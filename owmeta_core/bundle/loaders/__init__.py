'''
Package for uploaders and downloaders of bundles
'''
import logging
from pkg_resources import iter_entry_points, DistributionNotFound


L = logging.getLogger(__name__)

LOADERS_GROUP = 'owmeta_core.loaders'


class LoadFailed(Exception):
    '''
    Thrown when a bundle could not be downloaded
    '''
    def __init__(self, bundle_id, loader, *args):
        '''
        Parameters
        ----------
        bundle_id : str
            ID of the bundle on which a download was attempted
        loader : Loader
            The loader that attempted to download the bundle
        args[0] : str
            Explanation of why the download failed
        *args[1:]
            Passed on to `Exception`
        '''
        msg = args[0]
        mmsg = 'Failed to load {} bundle with loader {}{}'.format(
                bundle_id, loader, ': ' + msg if msg else '')
        super(LoadFailed, self).__init__(mmsg, *args[1:])


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
        LOADER_CLASSES.add(cls)


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
        UPLOADER_CLASSES.add(cls)


def load_entry_point_loaders():
    for entry_point in iter_entry_points(group=LOADERS_GROUP):
        try:
            entry_point.load().register()
        except DistributionNotFound:
            # This is expected...there's no pre-filtering of entry points for when extras
            # aren't installed...maybe to allow for optional depend on extras
            L.debug('Not adding loader %s due to failure in package resources resolution',
                    entry_point, exc_info=True)
        except Exception:
            L.warning('Unable to register loader %s', entry_point.name,
                    exc_info=L.isEnabledFor(logging.DEBUG))


LOADER_CLASSES = set()


UPLOADER_CLASSES = set()
