from owmeta_core.capabilities import FilePathProvider, FilePathCapability
from owmeta_core.data_trans.local_file_ds import LocalFileDataSource


def test_accept_provider():
    class Provider(FilePathProvider):
        def file_path(self):
            return 'tests'

    lfds = LocalFileDataSource()
    lfds.accept_capability_provider(FilePathCapability(), Provider())
    assert lfds.basedir() == 'tests'
