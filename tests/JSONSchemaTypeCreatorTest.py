from owmeta_core.dataobject import DataObject
from owmeta_core.datasource import DataSource, Informational
import owmeta_core.dataobject_property as DP
from owmeta_core.json_schema import DataSourceTypeCreator


def test_create_and_lookup_name():
    cut = DataSourceTypeCreator('Test', {'type': 'object'}, module=__name__)
    annotated_schema = cut.annotate()
    assert 'Test' == DataSourceTypeCreator.retrieve_type(annotated_schema).__name__


def test_create_and_retrieve_type():
    cut = DataSourceTypeCreator('Test', {'type': 'object'}, module=__name__)
    annotated_schema = cut.annotate()
    assert issubclass(DataSourceTypeCreator.retrieve_type(annotated_schema), DataSource)


def test_create_property_is_informational():
    cut = DataSourceTypeCreator('Test',
            {'type': 'object',
             'properties': {'toast': {'type': 'number'}}}, module=__name__)
    annotated_schema = cut.annotate()
    typ = DataSourceTypeCreator.retrieve_type(annotated_schema)
    assert isinstance(typ.toast, Informational)


def test_create_property_type_is_DatatypeProperty():
    cut = DataSourceTypeCreator('Test',
            {'type': 'object',
             'properties': {'toast': {'type': 'number'}}}, module=__name__)
    annotated_schema = cut.annotate()
    typ = DataSourceTypeCreator.retrieve_type(annotated_schema)
    assert typ.toast.property_type == 'DatatypeProperty'


def test_create_property_type_is_ObjectProperty():
    cut = DataSourceTypeCreator('Test',
            {'type': 'object',
             'properties': {'toast': {'type': 'object'}}}, module=__name__)
    annotated_schema = cut.annotate()
    typ = DataSourceTypeCreator.retrieve_type(annotated_schema)
    assert typ.toast.property_type == 'ObjectProperty'


def test_create_property_type_is_ObjectProperty_with_DatatypeProperty():
    cut = DataSourceTypeCreator('Test',
            {'type': 'object',
             'properties': {'toast': {'type': 'object',
                 'properties': {'jam': {'type': 'number'}}}}}, module=__name__)
    annotated_schema = cut.annotate()
    typ = DataSourceTypeCreator.retrieve_type(annotated_schema, '/properties/toast')
    assert issubclass(typ.jam.property, DP.DatatypeProperty)


def test_create_property_type_is_ObjectProperty_with_ObjectProperty():
    cut = DataSourceTypeCreator('Test',
            {'type': 'object',
             'properties': {'toast': {'type': 'object',
                 'properties': {'jam': {'type': 'object'}}}}}, module=__name__)
    annotated_schema = cut.annotate()
    typ = DataSourceTypeCreator.retrieve_type(annotated_schema, '/properties/toast')
    assert issubclass(typ.jam.property, DP.ObjectProperty)


def test_create_with_DataObject_ref():
    cut = DataSourceTypeCreator('Test',
            {'type': 'object',
             'properties': {'toast': {'$ref': '#/definitions/ref_test'}},
             'definitions': {'ref_test': {'type': 'object'}}}, module=__name__)
    annotated_schema = cut.annotate()
    typ = DataSourceTypeCreator.retrieve_type(annotated_schema, '/properties/toast')
    assert issubclass(typ, DataObject) and typ.__name__ == 'RefTest'
