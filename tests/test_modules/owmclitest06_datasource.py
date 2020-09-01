from owmeta_core.datasource import DataSource


class TestDataSource(DataSource):
    base_namespace = 'http://example.org/schema/'
    class_context = 'http://example.org/test-data-source'
