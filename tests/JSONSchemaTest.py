import pytest
from owmeta_core.json_schema import resolve_fragment


def test_resolve_fragment_percent_encoded():
    assert "blas" == resolve_fragment({" ": "blas"}, '#/%20')


def test_tilde_escape():
    assert "blas" == resolve_fragment({"~": "blas"}, '#/~0')


def test_tilde_error():
    '''
    It may be OK for now to just pass-through, but in the off-chance they add more tilde
    escapes, better to ensure we're coding per spec now.
    '''
    with pytest.raises(ValueError):
        resolve_fragment({"~": "blas"}, '#/~')


def test_tilde_01_correct():
    '''
    Make sure that we resolve that example from the spec, '~01', correctly into '~1'
    rather than '/'
    '''
    assert "blas" == resolve_fragment({"~1": "blas"}, '#/~01')


def test_rfc():
    document = {
        "foo": ["bar", "baz"],
        "": 0,
        "a/b": 1,
        "c%d": 2,
        "e^f": 3,
        "g|h": 4,
        "i\\j": 5,
        "k\"l": 6,
        " ": 7,
        "m~n": 8
    }

    tests = {
        '#': document,
        '#/foo': ["bar", "baz"],
        '#/foo/0': "bar",
        '#/': 0,
        '#/a~1b': 1,
        '#/c%25d': 2,
        '#/e%5Ef': 3,
        '#/g%7Ch': 4,
        '#/i%5Cj': 5,
        '#/k%22l': 6,
        '#/%20': 7,
        '#/m~0n': 8,
    }

    for fragment, expected in tests.items():
        print("test_rfc", fragment, expected)
        assert expected == resolve_fragment(document, fragment)


def test_sequence():
    assert 'playing' == resolve_fragment(['daft', 'punk', 'is', 'playing', 'at', 'my', 'house'], '#/3')


def test_sequence_dash_fail():
    with pytest.raises(ValueError):
        resolve_fragment(['daft', 'punk'], '#/-')


def test_lookup_fail():
    with pytest.raises(LookupError):
        resolve_fragment([], '#/1')


def test_lookup_empty_fragment():
    with pytest.raises(LookupError):
        resolve_fragment([], '#/1')
