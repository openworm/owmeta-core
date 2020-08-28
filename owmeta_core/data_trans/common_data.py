'''
Variables common to several `~owmeta_core.datasource.DataSource` and
`~owmeta_core.datasource.DataTranslator` implementations
'''
from rdflib.namespace import Namespace

from .. import BASE_SCHEMA_URL, BASE_DATA_URL

TRANS_NS = Namespace(BASE_SCHEMA_URL + '/translators/')
'''
Namespace for translators in owmeta-core. Not for use by packages downstream of
owmeta-core
'''

DS_NS = Namespace(BASE_SCHEMA_URL + '/data_sources/')
'''
Namespace for data sources in owmeta-core. Not for use by packages downstream of
owmeta-core
'''

DS_DATA_NS = Namespace(BASE_DATA_URL + '/data_sources/')
'''
Namespace for data sources in owmeta-core. Not for use by packages downstream of
owmeta-core
'''
