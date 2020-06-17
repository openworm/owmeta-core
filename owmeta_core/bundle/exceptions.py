class NotABundlePath(Exception):
    '''
    Thrown when a given path does not point to a valid bundle directory tree or bundle
    archive
    '''
    def __init__(self, path, explanation):
        message = '"{}" is not a bundle path: {}'.format(path, explanation)
        super(NotABundlePath, self).__init__(message)
        self.path = path


class MalformedBundle(NotABundlePath):
    '''
    Thrown when a given path does points to a bundle directory or archive is malformed
    '''


class BundleNotFound(Exception):
    '''
    Thrown when a bundle cannot be found on a local or remote resource with the given
    parameters.
    '''
    def __init__(self, bundle_id, msg=None, version=None):
        '''
        Parameters
        ----------
        bundle_id : str
            ID of the bundle that was sought
        msg : str, optional
            An explanation of why the bundle could not be found
        version : int, optional
            Version number of the bundle
        '''
        msg = 'Missing bundle "{}"{}{}'.format(bundle_id,
                '' if version is None else ' at version ' + str(version),
                ': ' + str(msg) if msg is not None else '')
        super(BundleNotFound, self).__init__(msg)


class InstallFailed(Exception):
    '''
    Thrown when a bundle installation fails to complete.

    You can assume that any intermediate bundle files have been cleaned up from the bundle
    cache
    '''


class UncoveredImports(InstallFailed):
    '''
    Thrown when a bundle to be installed has declared imports but is missing dependencies
    to cover those imports
    '''
    def __init__(self, imports):
        '''
        Parameters
        ----------
        imports : list of URIRef
            List of imports declared for a bundle which are not covered by any of the
            bundle's dependencies
        '''
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


class FetchTargetIsNotEmpty(FetchFailed):
    '''
    Thrown when the target directory of a fetch is not empty
    '''
    def __init__(self, target):
        msg = 'Bundle fetch target directory, "%s", is not empty' % target
        super(FetchTargetIsNotEmpty, self).__init__(msg)
        self.directory = target


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


class NoRemoteAvailable(Exception):
    '''
    Thrown when we need a remote and we don't have one
    '''


class NotADescriptor(Exception):
    '''
    Thrown when a given file, string, or other object is offered as a descriptor, but does
    not represent a `Descriptor`
    '''


class DeployFailed(Exception):
    '''
    Thrown when bundle deployment fails for an apparently valid bundle
    '''


class NoAcceptableUploaders(DeployFailed):
    '''
    Thrown when, for all selected `Remotes <Remote>`, no `Uploaders <Uploader>` report
    that they can upload a given bundle
    '''
    def __init__(self, bundle_path):
        super(NoAcceptableUploaders, self).__init__(
                'Could not upload "%s" because no uploaders could handle it' %
                bundle_path)
        self.bundle_path = bundle_path
