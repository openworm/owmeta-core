from .capability import Capability, Provider


class FilePathCapability(Capability):
    '''
    Provides a file path where named files can be retrieved.

    This capability may be needed when files are referred to that aren't necessarily
    stored on the local machine, or which on the local machine, but only in non-portable
    locations (e.g., a home directory).
    '''


class FilePathProvider(Provider):
    '''
    Provides the `FilePathCapability`
    '''

    provided_capabilities = [FilePathCapability()]

    def file_path(self):
        '''
        The needed file path
        '''
        raise NotImplementedError()


class CacheDirectoryCapability(Capability):
    '''
    Capability that provides a cache directory.

    The provider of this capability must be capable of persisting effectively distinct
    directories for each `Capable` which needs this capability. The provider must permit
    depositing files in the directory by the current effective user.
    '''


class CacheDirectoryProvider(Provider):
    '''
    Provides the `CacheDirectoryCapability`
    '''

    provided_capabilities = [CacheDirectoryCapability()]

    def clear(self, cache_key):
        '''
        Clear the cache directory for the `Capable`.

        Should remove the directory itself, if possible.
        '''
        raise NotImplementedError

    def cache_directory(self, cache_key):
        '''
        Return the cache directory path

        Parameters
        ----------
        cache_key : str
            The key for the cache entry

        Returns
        -------
        str
            The cache directory
        '''
        raise NotImplementedError


class TemporaryDirectoryCapability(Capability):
    '''
    Provides new, empty temporary directories
    '''


class TemporaryDirectoryProvider(Provider):
    '''
    Provides the `TemporaryDirectoryCapability`
    '''

    provided_capabilities = [TemporaryDirectoryCapability()]

    def temporary_directory(self):
        '''
        Return the path of a new, empty temporary directory. The receiver of the temporary
        directory should delete the directory when they're done with it.

        Returns
        -------
        str
            The temporary directory path
        '''
        raise NotImplementedError


class OutputFilePathCapability(Capability):
    '''
    Provides a file path where named files can be put
    '''


class OutputFilePathProvider(Provider):
    '''
    Provides the `OutputFilePathCapability`
    '''

    provided_capabilities = [OutputFilePathCapability()]

    def output_file_path(self):
        '''
        The needed file path
        '''
        raise NotImplementedError()

# Possible other capabilities:
# - http/socks proxy
# - user name / password
