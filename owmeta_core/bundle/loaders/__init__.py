'''
Package for uploaders and downloaders of bundles
'''
import logging
from pkg_resources import iter_entry_points


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


for entry_point in iter_entry_points(group=LOADERS_GROUP):
    try:
        entry_point.load().register()
    except Exception:
        L.warning('Unable to register loader %s', entry_point.name)
