import rdflib
from owmeta_core.datasource import DataTranslator


EX = rdflib.Namespace('http://example.org/')
LABEL = 'stuff and things'


class DT(DataTranslator):
    class_context = EX.context
    translator_identifier = EX.trans1

    def translate(self, src):
        res = self.make_new_output((src,), ident=EX.out)
        res.rdfs_label(LABEL)
        return res
