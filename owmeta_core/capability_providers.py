'''
Classes for managing things in the owmeta-core project directory, typically named .owm
'''
import hashlib
from os import makedirs, getcwd
from os.path import join as pth_join, exists

from .capabilities import OutputFilePathProvider, FilePathProvider, CacheDirectoryProvider
from .datasource import DataSource


class WorkingDirectoryProvider(FilePathProvider):
    '''
    Provides file paths from the current working directory for
    `.data_trans.local_file_ds.LocalFileDataSource` instances.
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cwd = getcwd()

    def provides_to(self, obj):
        from owmeta_core.data_trans.local_file_ds import LocalFileDataSource
        file_name = obj.file_name.one()
        if not file_name:
            return None
        if (isinstance(obj, LocalFileDataSource) and
                exists(pth_join(self.cwd, file_name))):
            return self
        return None

    def file_path(self):
        return self.cwd


class SimpleDataSourceDirProvider(OutputFilePathProvider):
    '''
    Provides a directory under the provided base directory
    '''

    def __init__(self, basedir):
        self._basedir = basedir

    def provides_to(self, obj):
        if isinstance(obj, DataSource):
            key = hashlib.sha256(self.identifier).hexdigest()
            return type(self).Helper(key)
        return None

    class Helper(OutputFilePathProvider):
        def __init__(self, parent, key):
            self._key = key
            self._parent = parent

        def file_path(self):
            # When a file path is requested, then we create one?
            #   Well, if we try to create the file path here, but that fails, there's not
            #   really a good recourse: we could have tried a different provider earlier in
            #   provides_to, but not now...  If, instead, we do it in provides_to, then we can
            #   just log the error and say "no, I can't provide a directory"
            #
            # An important point about the mapping from the RDF graph:
            #   We're mapping from the RDF graph here to a single directory, so what we're
            #   implicitly saying is that there's one and only one set of files corresponding
            #   to the data source. For a work-in-progress project, this is really
            #   important...also the data could change over time, right? How would we deal
            #   with a different version of the data still pointing to the project directory?
            #   We aren't worried about that because everything in the project is WIP. What
            #   you would worry about is whether you could have multiple WIP versions for a
            #   given data source...that's not as clear cut to me
            #
            # create a directory
            # create an index entry
            #   the index stores the mapping to the directory
            #
            # what if the directory creation fails?
            #   Then we won't have a directory and we won't create the index entry
            # what if the index entry creation fails?
            #   Then we need to clean delete the directory before propagating the exception
            #       What if the directory clean up fails?
            #           Then we need to report that as well as the original exception --
            #           Python will handle that for us though.
            return pth_join(self._parent._basedir, self._key)


class SimpleCacheDirectoryProvider(CacheDirectoryProvider):
    '''
    Provides a directory for caching remote resources as local files
    '''

    def __init__(self, cache_directory, **kwargs):
        super().__init__(**kwargs)
        self._cache_directory = cache_directory

    def provides_to(self, obj):
        return self

    def cache_directory(self, cache_key):
        res = pth_join(self._cache_directory, cache_key)
        makedirs(res)
        return res
