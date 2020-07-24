from rdflib.namespace import Namespace

from .. import BASE_CONTEXT

from .common_data import DS_NS
from .http_ds import HTTPFileDataSource


class XLSXHTTPFileDataSource(HTTPFileDataSource):
    class_context = BASE_CONTEXT

    rdf_namespace = Namespace(DS_NS['XLSXHTTPFileDataSource#'])
