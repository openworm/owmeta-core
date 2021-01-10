import logging
import pytest
import transaction
from owmeta_core import BASE_CONTEXT, connect
from owmeta_core.data import Data
from owmeta_core.dataobject import DataObject, PythonClassDescription, PythonModule
from owmeta_core.context import Context, ClassContext, IMPORTS_CONTEXT_KEY

cls_ctx = ClassContext('http://example.org/test_class_context',
            imported=(BASE_CONTEXT,))

L = logging.getLogger(__name__)


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


@pytest.fixture
def context(connection):
    yield connection(Context)('http://example.org/test_ctx')


SUBCLASSES = dict()


class _ClassDescription(PythonClassDescription):
    class_context = cls_ctx

    def resolve_class(self):
        L.debug('_ClassDescription resolving class %s', self.name())
        return SUBCLASSES.get(self.name())


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


def test_load_instances_subclasses(context, subclasses_1):
    assert 100 == len(set(context.stored(DataObject)().load()))


def test_declare_with_identifier(context):
    class _Class(DataObject):
        class_context = cls_ctx

    for ctr in range(1000):
        context(_Class)(ident=f'http://example.org/object_{ctr}')


@pytest.fixture
def declare_1(context):
    class _Class(DataObject):
        class_context = cls_ctx
    for ctr in range(1000):
        context(_Class)(ident=f'http://example.org/object_{ctr}')

    return _Class


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
