from contextlib import contextmanager
import hashlib
from unittest.mock import patch, ANY
import re

import pytest

from owmeta_core.bundle import URLConfig
from owmeta_core.bundle.loaders import LoadFailed
from owmeta_core.bundle.loaders.http import HTTPBundleLoader


def test_can_load_from_http():
    assert HTTPBundleLoader.can_load_from(URLConfig('http://'))


def test_can_load_from_https():
    assert HTTPBundleLoader.can_load_from(URLConfig('https://'))


def test_cannot_load_from_ftp():
    assert not HTTPBundleLoader.can_load_from(URLConfig('ftp://'))


def test_cannot_load_from_None():
    assert not HTTPBundleLoader.can_load_from(None)


def test_cannot_load_in_index_with_no_releases_no_version_provided():
    with successful_get({'test_bundle': {}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle')


def test_error_response_on_index_load():
    with patch('requests.Session') as Session:
        Session().get().status_code = 500
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle')


def test_cannot_load_in_index_with_releases_but_no_url_and_no_version_provided():
    with successful_get({'test_bundle': {'1': ''}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle')


def test_cannot_load_in_index_with_releases_but_bad_url_no_version_provided():
    with successful_get({'test_bundle': {'1': 'http://'}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle')


def test_can_load_in_index_with_releases_no_version_provided():
    with successful_get({'test_bundle': {'1': {'url': 'http://some_host'}}}):
        cut = HTTPBundleLoader('index_url')
        assert cut.can_load('test_bundle')


def test_can_load_in_index_with_releases_bad_bundle_info():
    with successful_get({'test_bundle': {'1': 'http://some_host'}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle')


def test_cannot_load_in_index_with_releases_but_no_matching_version_provided():
    with successful_get({'test_bundle': {'1': 'http://some_host'}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle', 2)


def test_cannot_load_in_index_with_no_releases():
    with successful_get({'test_bundle': {}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle', 2)


def test_cannot_load_not_in_index():
    with successful_get({}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle', 2)


def test_can_load_in_index_with_releases_and_matching_version_provided():
    with successful_get({'test_bundle': {'1': {'url': 'http://some_host'}}}):
        cut = HTTPBundleLoader('index_url')
        assert cut.can_load('test_bundle', 1)


def test_can_load_index_missing_url(bundle_archive):
    with successful_get({'test_bundle': {'1': {'hashes': {'sha224': 'deadbeef'}}}}):
        cut = HTTPBundleLoader('index_url')
        assert not cut.can_load('test_bundle')


def test_bundle_versions_multiple():
    with successful_get({'test_bundle': {
            '1': 'http://some_host',
            '2': 'http://some_host'}}):
        cut = HTTPBundleLoader('index_url')
        assert set(cut.bundle_versions('test_bundle')) == set([1, 2])


def test_bundle_versions_skips_non_int():
    with successful_get({'test_bundle': {
            '1': 'http://some_host',
            'oops': 'http://some_host',
            '2': 'http://some_host'}}):
        cut = HTTPBundleLoader('index_url')
        assert set(cut.bundle_versions('test_bundle')) == set([1, 2])


def test_load_fail_no_info():
    with successful_get({}):
        cut = HTTPBundleLoader('index_url')
        with pytest.raises(LoadFailed, match='not in.*index'):
            cut.load('test_bundle')


def test_load_fail_wrong_type_of_info():
    with successful_get({'test_bundle': ["wut"]}):
        cut = HTTPBundleLoader('index_url')
        with pytest.raises(LoadFailed, match='type.*bundle info'):
            cut.load('test_bundle')


def test_load_fail_no_valid_release_number():
    with successful_get({'test_bundle': {'smack': 'down'}}):
        cut = HTTPBundleLoader('index_url')
        with pytest.raises(LoadFailed,
                match=re.compile('no releases found', re.I)):
            cut.load('test_bundle')


def test_load_fail_no_valid_bundle_url():
    with successful_get({'test_bundle': {'1': {'url': 'down'}}}):
        cut = HTTPBundleLoader('index_url')
        with pytest.raises(LoadFailed,
                match=re.compile('valid url', re.I)):
            cut.load('test_bundle')


def test_index_missing_url(bundle_archive):
    with successful_get({'test_bundle': {'1': {'hashes': {'sha224': 'deadbeef'}}}}):
        cut = HTTPBundleLoader('index_url')
        with pytest.raises(LoadFailed, match=re.compile('valid.*url', re.I)):
            cut.load('test_bundle')


def test_load_no_cachedir():
    from io import BytesIO
    bundle_contents = b'bytes bytes bytes'
    bundle_hash = hashlib.sha224(bundle_contents).hexdigest()

    with successful_get({'test_bundle': {'1': {'url': 'http://some_host',
                                               'hashes': {'sha224': bundle_hash}}}}) as get, \
            patch('owmeta_core.bundle.loaders.http.Unarchiver') as Unarchiver:
        cut = HTTPBundleLoader('index_url')
        cut.base_directory = 'bdir'

        get().raw.read.return_value = bundle_contents
        cut.load('test_bundle')
        Unarchiver().unpack.assert_called_with(MatchingBytesIO(BytesIO(b'bytes bytes bytes')), 'bdir')


def test_load_cachedir(bundle_archive, tempdir):
    with open(bundle_archive.archive_path, 'rb') as bf:
        bundle_hash = hashlib.sha224(bf.read()).hexdigest()
    with successful_get({'test_bundle': {'1': {'url': 'http://some_host',
                                               'hashes': {'sha224': bundle_hash}}}}) as get, \
            patch('owmeta_core.bundle.loaders.http.Unarchiver') as Unarchiver:
        cut = HTTPBundleLoader('index_url', cachedir=tempdir)
        cut.base_directory = 'bdir'
        with open(bundle_archive.archive_path, 'rb') as bf:
            get().iter_content.return_value = [bf.read()]
        cut.load('test_bundle')
        Unarchiver().unpack.assert_called_with(ANY, 'bdir')


def test_load_urlconfig():
    cut = HTTPBundleLoader(URLConfig('index_url'))
    assert cut.index_url == 'index_url'


@contextmanager
def successful_get(body):
    with patch('owmeta_core.bundle.loaders.http.requests') as requests:
        requests.Session().get().json.return_value = body
        requests.Session().get().status_code = 200
        yield requests.Session().get


class MatchingBytesIO(object):
    def __init__(self, bio):
        self.bio = bio

    def __eq__(self, other):
        return self.bio.getvalue() == other.getvalue()
