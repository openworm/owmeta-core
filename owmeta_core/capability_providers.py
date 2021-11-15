'''
Classes for managing things in the owmeta-core project directory, typically named .owm
'''
import hashlib
from os import makedirs, getcwd, rename
from os.path import join as pth_join, exists
from random import getrandbits
from shutil import rmtree
import logging
from tempfile import mkdtemp

from .capabilities import (OutputFilePathProvider,
        FilePathProvider,
        FilePathCapability,
        CacheDirectoryProvider,
        TemporaryDirectoryProvider)
from .datasource import DataSource
from .file_lock import lock_file
from .utils import FCN


L = logging.getLogger(__name__)


class WorkingDirectoryProvider(FilePathProvider):
    '''
    Provides file paths from the current working directory for
    `.data_trans.local_file_ds.LocalFileDataSource` instances.
    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cwd = getcwd()

    def provides_to(self, obj, cap):
        from .data_trans.local_file_ds import LocalFileDataSource
        file_name = obj.file_name.one()
        if not file_name:
            return None
        if (isinstance(obj, LocalFileDataSource) and
                exists(pth_join(self.cwd, file_name))):
            return self
        return None

    def file_path(self):
        return self.cwd


class TransactionalDataSourceDirProvider(OutputFilePathProvider, FilePathProvider):
    '''
    Provides a DataSourceDirectoryProvider with transactional semantics.

    Provides a `TDSDPHelper` for `.DataSource` objects, indexed by the `DataSource`
    identifier. If asked to provide a `.FilePathCapability` (i.e, a directory for input),
    and the `DataSource` is a `~.data_trans.local_file_ds.LocalFileDataSource`, then we'll
    check that a file named with the value of `~.LocalFileDataSource.file_name` is in the
    provided directory.
    '''
    def __init__(self, basedir, transaction_manager):
        self._basedir = basedir
        self.transaction_manager = transaction_manager
        self._providers = dict()

    def provides_to(self, obj, cap):
        from .data_trans.local_file_ds import LocalFileDataSource
        if isinstance(obj, DataSource):
            key = hashlib.sha256(obj.identifier.encode('utf-8')).hexdigest()
            provider = self._providers.get(key)
            if provider is None:
                provider = TDSDPHelper(self._basedir, key, self.transaction_manager)
            if cap == FilePathCapability():
                # We need the file to already exist for FilePathCapability, so here we
                # check for that
                provider_file_path = provider.file_path()
                if not exists(provider_file_path):
                    return None
                if isinstance(obj, LocalFileDataSource):
                    file_name = obj.file_name.one()
                    if not exists(pth_join(provider_file_path, file_name)):
                        return None
            self._providers[key] = provider
            return provider
        return None


class TDSDPHelper(FilePathProvider, OutputFilePathProvider):
    '''
    This provider relies on the `transaction` library's machinery to manage the
    transaction.

    Consistency is NOT guaranteed in all cases: in particular, this provider uses a
    file-based locking mechanism with a `"lock file" <.file_lock.lock_file>` in the given
    base directory which, if it's deleted during the two-phase commit process, removes the
    isolation of the changes made in the directory.
    '''

    def __init__(self, basedir, key, transaction_manager):
        self._basedir = basedir
        self._key = key
        self.transaction_manager = transaction_manager

        self._uncommitted_path = None
        self._dm_sort_key = None
        self._file_lock = None
        self._committed_path = pth_join(self._basedir, self._key)
        self._prev_version_path = f"{self._committed_path}.prev"
        self._lock_file_name = f"{self._committed_path}.lock"
        self._transaction = None

    def file_path(self):
        if self._uncommitted_path is not None:
            return self._uncommitted_path
        else:
            return self._committed_path

    def output_file_path(self):
        if self._uncommitted_path is None:
            for attempt in range(3):
                try:
                    random_suffix = getrandbits(32)
                    path = f"{self._committed_path}.uncommitted.{random_suffix}"
                    makedirs(path)
                except OSError:
                    pass
                else:
                    break
            else: # no break
                raise Exception('Unable to create output directory')

            self._uncommitted_path = path
            # Join the transaction so if it fails our abort method will get called so
            # we can delete the "uncommitted" directory
            self._file_lock = lock_file(self._lock_file_name, unique_key=random_suffix.to_bytes(4, 'little'))
            self._transaction = self.transaction_manager.get()
            self._transaction.join(self)
        return self._uncommitted_path

    def tpc_begin(self, transaction):
        self._transaction = transaction

    def commit(self, transaction):
        self._file_lock.acquire()
        if exists(self._committed_path):
            rename(self._committed_path, self._prev_version_path)
        rename(self._uncommitted_path, self._committed_path)
        self._uncommitted_path = None

    def tpc_vote(self, transaction):
        if transaction is not self._transaction:
            raise Exception('tcp_vote called with the wrong transaction: given'
                    f' {transaction}, but expected {self._transaction}')

    def tpc_abort(self, transaction):
        try:
            if exists(self._committed_path):
                rmtree(self._committed_path)
            if exists(self._prev_version_path):
                rename(self._prev_version_path, self._committed_path)
        except Exception:
            L.error('Received exception in tpc_abort for transaction for %s,'
                    ' back from %s',
                    self._committed_path, self._prev_version_path,
                    exc_info=True)
        finally:
            self._release_lock()

    def abort(self, transaction):
        rmtree(self._uncommitted_path)
        self._uncommitted_path = None

    def _handle_rmtree_error(self, retry, path, exc_info):
        # abort and finish should "never fail" so we just log errors
        L.error('Unable to delete %s for transaction commit/abort', path,
                exc_info=exc_info)

    def tpc_finish(self, transaction):
        try:
            if exists(self._prev_version_path):
                rmtree(self._prev_version_path, onerror=self._handle_rmtree_error)
        except Exception:
            L.error('Received exception in tpc_finish for transaction for %s,'
                    ' removing %s,',
                    self._committed_path, self._prev_version_path,
                    exc_info=True)
        finally:
            self._release_lock()

    def _release_lock(self):
        try:
            self._file_lock.release()
        except FileNotFoundError:
            L.error('Lock file was deleted before being released: directory contents may be'
                    ' inconsistent', exc_info=True)
        except PermissionError:
            L.error('Lock file could not be released due to a permissions error: correct'
                    ' file system permissions, check directory contents, and delete the'
                    ' lock file.', exc_info=True)
        except Exception:
            L.error('Unknown error during lock file release: directory contents may be '
                    ' inconsistent', exc_info=True)

    def sortKey(self):
        '''
        See Also
        --------
        transaction.interfaces.IDataManager
        '''
        if self._dm_sort_key is None:
            if self._uncommitted_path is None:
                raise Exception('output_file_path must be called before a sort key is created')
            cname = FCN(type(self))
            self._dm_sort_key = f'{cname}{self._uncommitted_path}'
        return self._dm_sort_key


class SimpleDataSourceDirProvider(OutputFilePathProvider):
    '''
    Provides a directory under the provided base directory
    '''

    def __init__(self, basedir):
        self._basedir = basedir

    def provides_to(self, obj, cap):
        if isinstance(obj, DataSource):
            key = hashlib.sha256(obj.identifier.encode('utf-8')).hexdigest()
            return type(self).Helper(self, key)
        return None

    class Helper(OutputFilePathProvider):
        def __init__(self, parent, key):
            self._key = key
            self._parent = parent

        def output_file_path(self):
            # When a file path is requested, then we create one?
            #   Well, if we try to create the file path here, but that fails, there's not
            #   really a good recourse: we could have tried a different provider earlier in
            #
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

    def provides_to(self, obj, cap):
        return self

    def cache_directory(self, cache_key):
        res = pth_join(self._cache_directory, cache_key)
        makedirs(res)
        return res


class SimpleTemporaryDirectoryProvider(TemporaryDirectoryProvider):
    '''
    Provides temporary directories under a given base directory
    '''
    def __init__(self, base_directory, suffix=None, prefix=None, **kwargs):
        super().__init__(**kwargs)
        self._base_directory = base_directory
        self._suffix = suffix
        self._prefix = prefix

    def provides_to(self, obj, cap):
        return self

    def temporary_directory(self):
        if not exists(self._base_directory):
            makedirs(self._base_directory)
        return mkdtemp(suffix=self._suffix, prefix=self._prefix, dir=self._base_directory)
