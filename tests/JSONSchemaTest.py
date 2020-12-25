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


def test_sequence():
    assert 'playing' == resolve_fragment(['daft', 'punk', 'is', 'playing', 'at', 'my', 'house'], '#/3')


def test_sequence_dash_fail():
    with pytest.raises(ValueError):
        resolve_fragment(['daft', 'punk'], '#/-')


def test_lookup_fail():
    with pytest.raises(LookupError):
        resolve_fragment([], '#/1')
