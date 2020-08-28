from contextlib import contextmanager
from rdflib.namespace import Namespace

from .. import BASE_CONTEXT
from ..datasource import Informational, DataSource


class FileDataSource(DataSource):

    class_context = BASE_CONTEXT

    md5 = Informational(display_name='MD5 hash')
    sha256 = Informational(display_name='SHA-256 hash')
    sha512 = Informational(display_name='SHA-512 hash')

    @contextmanager
    def file_contents(self):
        raise NotImplementedError()

    def update_hash(self, algorithm):
        import hashlib
        hsh = hashlib.new(algorithm)
        with self.file_contents() as f:
            hsh.update(f.read())
        getattr(self, algorithm).set(hsh.hexdigest())
