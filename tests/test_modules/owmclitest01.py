from owmeta_core import __version__
from owmeta_core.data_trans.local_file_ds import LocalFileDataSource as LFDS
from owmeta_core.datasource import DataTranslator


__distribution__ = dict(name='owmeta-core',
                        version=__version__)


class DT2(DataTranslator):
    class_context = 'http://example.org/context'
    input_type = LFDS
    output_type = LFDS
    translator_identifier = 'http://example.org/trans1'

    def translate(self, source):
        print(source.full_path(), end='')
        return self.make_new_output((source,), file_name='Outfile')
