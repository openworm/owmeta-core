from os.path import join as pth_join
from contextlib import contextmanager
import csv

from rdflib.namespace import Namespace

from .. import BASE_CONTEXT
from ..datasource import Informational, DataTranslator

from .local_file_ds import LocalFileDataSource
from .http_ds import HTTPFileDataSource


class CSVDataSource(LocalFileDataSource):
    '''
    A CSV file data source
    '''

    class_context = BASE_CONTEXT

    csv_file_name = Informational(display_name='CSV file name',
                                  also=LocalFileDataSource.file_name)

    csv_header = Informational(display_name='Header column names', multiple=False)

    csv_field_delimiter = Informational(display_name='CSV field delimiter')


class CSVHTTPFileDataSource(HTTPFileDataSource):
    '''
    A CSV file retrieved over HTTP
    '''

    class_context = BASE_CONTEXT

    csv_header = CSVDataSource.csv_header

    csv_field_delimiter = CSVDataSource.csv_field_delimiter


class CSVDataTranslator(DataTranslator):
    '''
    A data translator which handles CSV files
    '''

    class_context = BASE_CONTEXT

    input_type = (CSVDataSource,)

    def make_reader(self, source, skipheader=True, dict_reader=False, skiplines=0, **kwargs):
        '''
        Make a CSV reader

        Parameters
        ----------
        source : CSVDataSource
            The data source to read from
        skipheader : bool
            If true, the first line read of the CSV file after the reader is created will
            not be returned from the reader
        dict_reader : bool
            If true, the reader will be a `~csv.DictReader`
        skiplines : int
            A number of lines to skip before creating the reader. Useful if the CSV file
            contains some commentary or other 'front matter'
        **kwargs
            Remaining arguments passed on to `~csv.reader` or `~csv.DictReader`
        '''
        params = dict()
        delim = source.csv_field_delimiter.one()

        if delim:
            params['delimiter'] = str(delim)

        params['skipinitialspace'] = True
        params.update(kwargs)

        @contextmanager
        def cm(skiplines, dict_reader):
            _skipheader = skipheader
            fname = source.full_path()
            with open(fname) as f:
                while skiplines > 0:
                    next(f)
                    skiplines -= 1
                if dict_reader:
                    if 'fieldnames' not in params:
                        _skipheader = False
                    reader = csv.DictReader(f, **params)
                else:
                    reader = csv.reader(f, **params)
                if _skipheader:
                    next(reader)
                yield reader
        return cm(skiplines, dict_reader)

    reader = make_reader
    ''' Alias to `make_reader` '''
