from os.path import join

from rdflib.namespace import Namespace

from .. import BASE_CONTEXT
from ..datasource import Informational
from ..capability import Capable, NoProviderGiven
from ..capabilities import FilePathCapability

from .file_ds import FileDataSource


class LocalFileDataSource(Capable, FileDataSource):
    '''
    File paths should be relative -- in general, path names on a given machine are not portable
    '''
    class_context = BASE_CONTEXT

    file_name = Informational(display_name='File name')
    torrent_file_name = Informational(display_name='Torrent file name')
    needed_capabilities = [FilePathCapability()]

    def __init__(self, *args, **kwargs):
        super(LocalFileDataSource, self).__init__(*args, **kwargs)
        self._base_path_provider = None

    def file_contents(self):
        if not self._base_path_provider:
            raise NoProviderGiven(FilePathCapability())

        return open(self.full_path(), 'b')

    def full_path(self):
        return join(self.basedir(), self.file_name.one())

    def accept_capability_provider(self, cap, provider):
        self._base_path_provider = provider

    def basedir(self):
        return self._base_path_provider.file_path()
