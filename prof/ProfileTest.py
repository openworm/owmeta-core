import logging
import pytest
import transaction
from owmeta_core import BASE_CONTEXT, connect
from owmeta_core.data import Data
from owmeta_core.dataobject import DataObject, PythonClassDescription, PythonModule
from owmeta_core.context import Context, ClassContext, IMPORTS_CONTEXT_KEY
from rdflib import Namespace as NS


cls_ctx = ClassContext('http://example.org/test_class_context',
            imported=(BASE_CONTEXT,))

L = logging.getLogger(__name__)


SUBCLASSES = dict()


def test_load_instances_subclasses(context, subclasses_1):
    assert 100 == len(set(context.stored(DataObject)().load()))


def test_declare_with_identifier(context):
    class _Class(DataObject):
        class_context = cls_ctx

    for ctr in range(1000):
        context(_Class)(ident=f'http://example.org/object_{ctr}')


def test_save(context, declare_1):
    with transaction.manager:
        context.save()


def test_load_instances_one_type(context, declare_1):
    count = 0
    for _ in context(declare_1)().load():
        count += 1
    assert count == 1000


def test_load_instances_one_type_len(context, declare_1):
    assert len(list(context(declare_1)().load())) == 1000


def test_RDFContextStore_triples_subset_distinct(multi_context_distinct, context):
    '''
    Query a subset of a larger graph by selecting imports within
    '''
    count = 0
    for t in context.stored.rdf_graph().triples((None, None, None)):
        count += 1
    assert count == 200


def test_RDFContextStore_triples_subset_shared(multi_context_shared, context):
    '''
    Query a subset of a larger graph by selecting imports within
    '''
    count = 0
    seen = set()
    for t in context.stored.rdf_graph().triples((None, None, None)):
        if t not in seen:
            count += 1
            seen.add(t)
    assert count == 100


@pytest.fixture
def multi_context_distinct(connection, context):
    imported_context = connection(Context)('http://example.org/jalacy')
    context.add_import(imported_context)
    ex = NS('http://example.org/')
    g = connection.rdf
    with transaction.manager:
        g0 = g.graph(context.identifier)
        for i in range(100):
            g0.add((ex[f"a_s{i}"], ex[f"a_p{i}"], ex[f"a_o{i}"]))
        g1 = g.graph(imported_context.identifier)
        for i in range(100):
            g1.add((ex[f"b_s{i}"], ex[f"b_p{i}"], ex[f"b_o{i}"]))

        for j in range(50):
            g2 = g.graph(ex[f"{i}"])
            for i in range(100):
                g2.add((ex[f"{j}_s{i}"], ex[f"{j}_p{i}"], ex[f"{j}_o{i}"]))
        context.save_imports()


@pytest.fixture
def multi_context_shared(connection, context):
    imported_context = connection(Context)('http://example.org/jalacy')
    context.add_import(imported_context)
    ex = NS('http://example.org/')
    g = connection.rdf
    with transaction.manager:
        g0 = g.graph(context.identifier)
        for i in range(100):
            g0.add((ex[f"s{i}"], ex[f"p{i}"], ex[f"o{i}"]))
        g1 = g.graph(imported_context.identifier)
        for i in range(100):
            g1.add((ex[f"s{i}"], ex[f"p{i}"], ex[f"o{i}"]))

        for j in range(50):
            g2 = g.graph(ex[f"{i}"])
            for i in range(100):
                g2.add((ex[f"s{i}"], ex[f"p{i}"], ex[f"o{i}"]))
        context.save_imports()


@pytest.fixture
def subclasses_1(connection, context):
    global SUBCLASSES, cls_ctx

    my_cls_ctx = ClassContext('http://example.org/my_test_class_context',
                imported=(cls_ctx, BASE_CONTEXT,))
    context.add_import(my_cls_ctx)

    class SCTestMeta(type(DataObject)):
        def declare_class_description(self):
            cd = _ClassDescription.contextualize(self.context)()

            mo = PythonModule.contextualize(self.context)()
            mo.name(self.__module__)

            cd.module(mo)
            cd.name(self.__name__)

            return cd

    cls = DataObject
    for c in range(100):
        cls = SCTestMeta(f'_subclass_{c}',
                (cls,),
                dict(class_context=my_cls_ctx))
        SUBCLASSES[cls.__name__] = cls
        context(cls)(key='test')
        connection.mapper.declare_python_class_registry_entry(cls)
    connection.mapper.declare_python_class_registry_entry(_ClassDescription)
    with transaction.manager:
        context.save(inline_imports=True)
        context.save_imports()
        connection.mapper.save()

    return cls


@pytest.fixture
def declare_1(context):
    class _Class(DataObject):
        class_context = cls_ctx
    for ctr in range(1000):
        context(_Class)(ident=f'http://example.org/object_{ctr}')

    return _Class


@pytest.fixture
def context(connection):
    yield connection(Context)('http://example.org/test_ctx')


@pytest.fixture
def connection(tmpdir):
    conn = connect(Data({'rdf.source': 'default',
        'rdf.store': 'FileStorageZODB',
        IMPORTS_CONTEXT_KEY: 'http://example.org/imports',
        'rdf.store_conf': f'{tmpdir}/test.db'}))
    try:
        yield conn
    finally:
        conn.disconnect()


class _ClassDescription(PythonClassDescription):
    class_context = cls_ctx

    def resolve_class(self):
        L.debug('_ClassDescription resolving class %s', self.name())
        return SUBCLASSES.get(self.name())
