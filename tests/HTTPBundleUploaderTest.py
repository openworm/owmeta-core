from os.path import join as p
import logging
from unittest.mock import patch
import re

import pytest

from owmeta_core.bundle.common import BUNDLE_ARCHIVE_MIME_TYPE
from owmeta_core.bundle.loaders.http import HTTPBundleUploader, HTTPSURLConfig


L = logging.getLogger(__name__)


def test_bundle_upload_directory(http_server, tempdir):
    '''
    Uploading a directory requires that we turn it into an archive first.
    '''
    cut = HTTPBundleUploader(http_server.url)
    with open(p(tempdir, 'random_file'), 'w') as f:
        f.write("smashing")

    cut(tempdir)

    req = http_server.requests.get()
    while req['method'] != 'POST':
        req = http_server.requests.get()

    assert req['headers']['content-type'] == BUNDLE_ARCHIVE_MIME_TYPE


@pytest.fixture
def mocked_upload_client(tempdir):
    testfile = p(tempdir, 'random_file')
    with open(testfile, 'w') as f:
        f.write("smashing")
    with patch('owmeta_core.bundle.loaders.http.http.client') as hc, \
            patch('owmeta_core.bundle.loaders.http.ensure_archive') as mock_ensure_archive:
        mock_ensure_archive().__enter__.return_value = testfile
        hc.HTTPConnection().request.side_effect = BrokenPipeError
        yield hc


def test_bundle_upload_broken_pipe_default_no_retry(mocked_upload_client):
    cut = HTTPBundleUploader('http://fakeyfakeurl')

    with pytest.raises(BrokenPipeError):
        cut(None)
    mocked_upload_client.HTTPConnection().request.assert_called_once()


def test_bundle_upload_broken_pipe_with_retry(mocked_upload_client):
    cut = HTTPBundleUploader('http://fakeyfakeurl', max_retries=3)

    with pytest.raises(BrokenPipeError):
        cut(None)
    assert mocked_upload_client.HTTPConnection().request.call_count == 4


def test_bundle_upload_broken_pipe_with_retry_logs(mocked_upload_client, caplog):
    cut = HTTPBundleUploader('http://fakeyfakeurl', max_retries=3)

    with pytest.raises(BrokenPipeError):
        cut(None)

    retry_logs = []
    for r in caplog.messages:
        if re.match('.*[Bb]undle.*retry.*', r):
            retry_logs.append(r)
    assert len(retry_logs) == 3
    assert '3' in retry_logs[0]
    assert '2' in retry_logs[1]
    assert '1' in retry_logs[2]


def test_bundle_upload_directory_to_https(https_server, tempdir):
    cut = HTTPBundleUploader(https_server.url, ssl_context=https_server.ssl_context)
    with open(p(tempdir, 'random_file'), 'w') as f:
        f.write("smashing")

    cut(tempdir)

    req = https_server.requests.get()
    while req['method'] != 'POST':
        req = https_server.requests.get()

    assert req['headers']['content-type'] == BUNDLE_ARCHIVE_MIME_TYPE


def test_bundle_upload_directory_to_https_by_urlconfig(https_server, tempdir):
    cut = HTTPBundleUploader(HTTPSURLConfig(
        https_server.url, ssl_context=https_server.ssl_context))
    with open(p(tempdir, 'random_file'), 'w') as f:
        f.write("smashing")

    cut(tempdir)

    req = https_server.requests.get()
    while req['method'] != 'POST':
        req = https_server.requests.get()

    assert req['headers']['content-type'] == BUNDLE_ARCHIVE_MIME_TYPE


def test_bundle_upload_archive(http_server, bundle_archive):
    cut = HTTPBundleUploader(http_server.url)

    cut(bundle_archive.archive_path)

    req = http_server.requests.get()
    while req['method'] != 'POST':
        req = http_server.requests.get()

    assert req['headers']['content-type'] == BUNDLE_ARCHIVE_MIME_TYPE
