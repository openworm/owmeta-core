from owmeta_core.datasource import DataTranslator
from rdflib.term import URIRef


class DT1(DataTranslator):
    class_context = 'http://example.org/context'
    translator_identifier = URIRef('http://example.org/trans1')

    def translate(self, source):
        pass



