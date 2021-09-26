from contextlib import contextmanager

from .. import BASE_CONTEXT
from ..datasource import Informational, DataSource


class FileDataSource(DataSource):
    '''
    This DataSource represents a "file", essentially a sequence of bytes with a name

    Attributes
    ----------
    source_file_path : :term:`path-like object`
        The file to commit for this datasource
    '''

    class_context = BASE_CONTEXT

    md5 = Informational(display_name='MD5 hash')
    sha256 = Informational(display_name='SHA-256 hash')
    sha512 = Informational(display_name='SHA-512 hash')

    def __init__(self, *args, **kwargs):
        super(FileDataSource, self).__init__(*args, **kwargs)
        self.source_file_path = None

    @contextmanager
    def file_contents(self):
        '''
        Returns a :term:`file object` for reading data from the file
        '''
        raise NotImplementedError()

    def update_hash(self, algorithm):
        '''
        Set a message digest property for the file

        Parameters
        ----------
        algorithm : str
            The name of the property and algorithm to update
        '''
        import hashlib
        hsh = hashlib.new(algorithm)
        with self.file_contents() as f:
            hsh.update(f.read())
        getattr(self, algorithm).set(hsh.hexdigest())
