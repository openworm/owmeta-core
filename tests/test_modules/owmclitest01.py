from owmeta_core.data_trans.local_file_ds import LocalFileDataSource as LFDS
from owmeta_core.datasource import DataTranslator


class DT2(DataTranslator):
    class_context = 'http://example.org/context'
    input_type = LFDS
    output_type = LFDS
    translator_identifier = 'http://example.org/trans1'

    def translate(self, source):
        with source.file_contents() as f:
            print("File contents:\n", f.read())

        res = self.make_new_output((source,), file_name='Outfile')
        res.source_file_path = source.full_path()
        return res
