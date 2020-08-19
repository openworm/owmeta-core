'''
Variables common to several `~owmeta_core.datasource.DataSource` and
`~owmeta_core.datasource.DataTranslator` implementations
'''
from rdflib.namespace import Namespace

TRANS_NS = Namespace('http://openworm.org/entities/translators/')
'''
Namespace for translators in owmeta-core. Not for use by packages downstream of
owmeta-core
'''

DS_NS = Namespace('http://schema.openworm.org/2020/07/data_sources/')
'''
Namespace for data sources in owmeta-core. Not for use by packages downstream of
owmeta-core
'''
