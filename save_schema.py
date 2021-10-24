from owmeta_core.command import OWM


owm = OWM()

for module in ('owmeta_core.datasource',
               'owmeta_core.dataobject',
               'owmeta_core.context_dataobject',
               'owmeta_core.collections',
               'owmeta_core.data_trans.csv_ds',
               'owmeta_core.data_trans.context_datasource',
               'owmeta_core.data_trans.file_ds',
               'owmeta_core.data_trans.excel_ds',
               'owmeta_core.data_trans.http_ds',
               'owmeta_core.data_trans.local_file_ds'):
    owm.save(module)
