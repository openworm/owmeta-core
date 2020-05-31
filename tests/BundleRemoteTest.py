from io import StringIO
from unittest.mock import patch, Mock

from owmeta_core.bundle import Remote, URLConfig
from owmeta_core.bundle.loaders.http import HTTPBundleLoader


def test_remote_str_no_configs():
    s = str(Remote('test'))
    assert '<none>' in s
    assert 'test' in s


def test_remote_repr():
    r = repr(Remote('test'))
    assert repr([]) in r
    assert repr('test') in r


def test_remote_add_config_no_dupe():
    uc = URLConfig('http://example.org/bluh')
    cut = Remote('test', (uc,))
    cut.add_config(uc)
    assert len(cut.accessor_configs) == 1


def test_remote_equality():
    uc = URLConfig('http://example.org/bluh')
    cut1 = Remote('test', (uc,))
    cut2 = Remote('test', (uc,))
    assert cut1 == cut2


def test_remote_inequality_by_accessors():
    uc = URLConfig('http://example.org/bluh')
    cut1 = Remote('test', (uc,))
    cut2 = Remote('test', ())
    assert cut1 != cut2


def test_remote_inequality_by_name():
    uc = URLConfig('http://example.org/bluh')
    cut1 = Remote('test1', (uc,))
    cut2 = Remote('test2', (uc,))
    assert cut1 != cut2


def test_write_read_remote_1():
    out = StringIO()
    r0 = Remote('remote')
    r0.write(out)
    out.seek(0)
    r1 = Remote.read(out)
    assert r0 == r1


def test_write_read_remote_2():
    out = StringIO()
    r0 = Remote('remote')
    r0.add_config(URLConfig('http://example.org/bundle_remote0'))
    r0.add_config(URLConfig('http://example.org/bundle_remote1'))
    r0.write(out)
    out.seek(0)
    r1 = Remote.read(out)
    assert r0 == r1


def test_get_http_url_loaders():
    '''
    Find loaders for HTTP URLs
    '''
    r0 = Remote('remote')
    r0.add_config(URLConfig('http://example.org/bundle_remote0'))
    for l in r0.generate_loaders():
        if isinstance(l, HTTPBundleLoader):
            return

    raise AssertionError('No HTTPBundleLoader was created')


def test_remote_generate_uploaders_skip():
    mock = Mock()
    with patch('owmeta_core.bundle.UPLOADER_CLASSES', [mock]):
        r0 = Remote('remote')
        r0.add_config(URLConfig('http://example.org/bundle_remote0'))
        for ul in r0.generate_uploaders():
            pass
    mock.can_upload_to.assert_called()


def test_remote_generate_uploaders_no_skip():
    mock = Mock()
    mock.can_upload_to.return_value = True
    ac = URLConfig('http://example.org/bundle_remote0')
    with patch('owmeta_core.bundle.UPLOADER_CLASSES', [mock]):
        r0 = Remote('remote')
        r0.add_config(ac)
        for ul in r0.generate_uploaders():
            pass
    mock.assert_called_with(ac)
