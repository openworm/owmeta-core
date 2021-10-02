from enum import Enum, unique, auto
import os
from os.path import join
import shutil

from .. import BASE_CONTEXT
from ..datasource import Informational
from ..capability import NoProviderGiven
from ..capabilities import FilePathCapability, OutputFilePathCapability
from ..capable_configurable import CapableConfigurable

from .file_ds import FileDataSource


@unique
class CommitOp(Enum):
    '''
    Indicates which operation to perform for "commiting" a local file. See
    `LocalFileDataSource`.
    '''
    RENAME = auto()
    ''' rename the source file to the target file '''

    COPY = auto()
    ''' copy the source file contents to the target file '''

    SYMLINK = auto()
    '''
    create a symbolic link to the file. This may not be allowed for unprivileged users on
    Windows machines
    '''

    HARDLINK = auto()
    '''
    create a hard-link to the file. This will not be valid in case the source and target
    file are on different file systems.
    '''


# Dev note: we combine the LocalFileDataSource with the DataSourceDirLoader (DSDL) and
# FilePathCapability to allow for a pattern of retrieving files that allows for a variety
# file retrieval methods (e.g., SFTP, HTTP) without defining a bunch of FileDataSource
# sub-classes, although, of course, that's still possible. The advantage is that we can
# create file and directory tree retrieval methods that work for a variety of kinds of
# data source. The DataSourceDirectoryProvider, a FilePathCapability provider, is
# responsible for providing directories retrieved by DSDLs. We use the capability
# framework to make this link since it is our general tool for filling local,
# non-shareable needs for objects (typically DataObjects).
#
# Despite the separation provided by the framework described above, DataSources will
# typically have some accession information like a record number or a URI attached with a
# property that relates closely with a given DSDL.
class LocalFileDataSource(CapableConfigurable, FileDataSource):
    '''
    File paths should be relative -- in general, path names on a given machine are not portable

    Attributes
    ----------
    commit_op : CommitOp
        The operation to use for commiting the file changes
    '''
    class_context = BASE_CONTEXT

    file_name = Informational(display_name='File name')
    torrent_file_name = Informational(display_name='Torrent file name')
    wanted_capabilities = [FilePathCapability(), OutputFilePathCapability()]

    def __init__(self, *args, commit_op=CommitOp.COPY, **kwargs):
        '''
        Parameters
        ----------
        commit_op : CommitOp, optional
            The operation to use for commiting the file changes. The default is
            `~CommitOp.COPY`
        '''
        # CapableConfigurable can call accept_capability_provider in __init__, so we set
        # these here so they're set to *something*
        self._base_path_provider = None
        self._output_file_path_provider = None
        super(LocalFileDataSource, self).__init__(*args, **kwargs)
        self.commit_op = commit_op

    def accept_capability_provider(self, cap, provider):
        if isinstance(cap, FilePathCapability):
            self._base_path_provider = provider
        elif isinstance(cap, OutputFilePathCapability):
            self._output_file_path_provider = provider
        else:
            super().accept_capability_provider(cap, provider)

    def file_contents(self):
        '''
        Returns an open file to be read from at ``<full_path>/<file_name>``

        This file should be closed when you are done with it. It may be used as a context
        manager
        '''
        return open(self.full_path(), 'br')

    def full_path(self):
        '''
        Returns the full path to the file
        '''
        return join(self.basedir(), self.file_name.one())

    def basedir(self):
        if not self._base_path_provider:
            raise NoProviderGiven(FilePathCapability(), self)
        return self._base_path_provider.file_path()

    def file_output(self):
        '''
        Returns an open file to be written to at ``<full_path>/<file_name>``

        This file should be closed when you are done with it. It may be used as a context
        manager
        '''
        return open(self.full_output_path(), 'bw')

    def full_output_path(self):
        '''
        Returns the full output path to the file
        '''
        return join(self.output_basedir(), self.file_name.one())

    def output_basedir(self):
        if not self._output_file_path_provider:
            raise NoProviderGiven(OutputFilePathCapability(), self)
        return self._output_file_path_provider.output_file_path()

    def after_transform(self):
        '''
        "Commits" the file by applying the operation indicated by `commit_op` to
        `source_file_path` so that it is accessible at `full_path`
        '''
        super(LocalFileDataSource, self).after_transform()
        if self.commit_op == CommitOp.SYMLINK:
            os.symlink(self.source_file_path, self.full_output_path())
        elif self.commit_op == CommitOp.HARDLINK:
            os.link(self.source_file_path, self.full_output_path())
        elif self.commit_op == CommitOp.RENAME:
            os.rename(self.source_file_path, self.full_output_path())
        elif self.commit_op == CommitOp.COPY:
            # We're asking for a bit of trouble here relying on the file system to
            # preserve permissions, but it *is* possible to ensure they're preserved
            # as-set by the transformer through this operation (at least as far as Python
            # standard lib provides) since you can set the source file to be on the same
            # file system as the target file.
            shutil.copy2(self.source_file_path, self.full_output_path())
        elif isinstance(self.commit_op, CommitOp):
            raise NotImplementedError(
                    f'The given commit_op value is not supported: {self.commit_op}')
        else:
            raise TypeError(f'The given commit_op value is not a CommitOp: {self.commit_op}')
