from rdflib.namespace import Namespace

from .. import BASE_CONTEXT
from ..mapper import mapped

from .common_data import DS_NS
from .http_ds import HTTPFileDataSource


@mapped
class XLSXHTTPFileDataSource(HTTPFileDataSource):
    class_context = BASE_CONTEXT

    rdf_namespace = Namespace(DS_NS['XLSXHTTPFileDataSource#'])
