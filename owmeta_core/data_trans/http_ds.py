from contextlib import contextmanager
from six.moves.urllib.request import urlopen

from .. import BASE_CONTEXT
from ..datasource import Informational

from .file_ds import FileDataSource


class HTTPFileDataSource(FileDataSource):

    class_context = BASE_CONTEXT

    url = Informational(display_name='URL')

    @contextmanager
    def file_contents(self):
        return urlopen(self.url.one())
