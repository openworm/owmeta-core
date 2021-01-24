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

    provided_capabilities = [FilePathCapability()]

    def clear(self):
        '''
        Clear the cache directory for the `Capable`.

        Should remove the directory itself, if possible.
        '''
        raise NotImplementedError

    def cache_directory(self):
        '''
        Return the cache directory path

        Returns
        -------
        str
            The cache directory
        '''
        raise NotImplementedError


# Possible other capabilities:
# - http/socks proxy
# - user name / password
