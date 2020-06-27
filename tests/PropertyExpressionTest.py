from unittest.mock import Mock

from rdflib.term import URIRef

from owmeta_core.dataobject_property import PropertyExpr


def test_simple():
    prop = Mock()
    prop.get_terms.return_value = ['v1']
    p = PropertyExpr([prop])
    assert list(p.to_terms()) == ['v1']


def test_or():
    prop1 = Mock()
    prop2 = Mock()
    prop1.get_terms.return_value = ['v1']
    prop2.get_terms.return_value = ['v2']
    p1 = PropertyExpr([prop1])
    p2 = PropertyExpr([prop2])
    terms = (p1 | p2).to_terms()
    assert list(terms) == ['v1', 'v2']


def test_extend_terms():
    prop = Mock()
    prop.rdf.triples_choices.return_value = \
        [(URIRef('v1'), URIRef('p'), URIRef('v1o')),
         (URIRef('v2'), URIRef('p'), URIRef('v2o'))]

    prop.get_terms.return_value = ['v1', 'v2']
    p = PropertyExpr([prop])
    assert list(p.a_property.to_terms()) == [URIRef('v1o'), URIRef('v2o')]


def test_extend_triples_choices_argument():
    prop = Mock()
    prop.rdf.triples_choices.return_value = []

    prop.get_terms.return_value = ['v1', 'v2']
    p = PropertyExpr([prop])
    list(p.a_property.to_terms())
    prop.rdf.triples_choices.assert_called_with((
        ['v1', 'v2'],
        prop.value_type.a_property.link,
        None))


def test_extend_to_dict():
    prop = Mock()
    prop.rdf.triples_choices.return_value = \
        [(URIRef('v1'), URIRef('v1p'), URIRef('v1o')),
         (URIRef('v2'), URIRef('v1p'), URIRef('v2o'))]

    prop.get_terms.return_value = ['v1', 'v2']
    p = PropertyExpr([prop])
    assert p.a_property.to_dict() == {URIRef('v1'): URIRef('v1o'),
                                      URIRef('v2'): URIRef('v2o')}


def test_to_dict():
    prop = Mock()
    prop.get_terms.return_value = ['v1', 'v2']
    p = PropertyExpr([prop])
    assert p.to_dict(multiple=True) == {prop.owner.identifier: {'v1', 'v2'}}


def test_or_to_dict():
    prop1 = Mock()
    prop2 = Mock()
    prop1.get_terms.return_value = ['v1', 'v2']
    prop2.get_terms.return_value = ['v3', 'v4']
    p1 = PropertyExpr([prop1])
    p2 = PropertyExpr([prop2])
    assert (p1 | p2).to_dict(multiple=True) == {
            prop1.owner.identifier: {'v1', 'v2'},
            prop2.owner.identifier: {'v3', 'v4'}}


def test_or_self_is_self():
    p = PropertyExpr([Mock()])
    assert (p | p) is p


def test_to_objects_prop_getitem():
    prup = Mock()
    prup.rdf.triples_choices.return_value = \
        [(URIRef('v1'), URIRef('v1p'), URIRef('v1o')),
         (URIRef('v2'), URIRef('v1p'), URIRef('v2o'))]
    a_property = Mock()
    a_property.link = URIRef('v1p')

    prup.get_terms.return_value = [URIRef('v1'), URIRef('v2')]
    p = PropertyExpr([prup])
    p.property(a_property)()
    assert p.to_objects()[0].property(a_property) == URIRef('v1o')
