from os.path import join as p

import pytest

from owmeta_core.bundle.loaders.http import HTTPBundleLoader, HTTPURLConfig


@pytest.mark.inttest
def test_cache_index_etag(http_bundle_server, tempdir):
    cache_dir = p(tempdir, 'cache')
    config = HTTPURLConfig(f'{http_bundle_server.url}/index.json',
            cache_dir=cache_dir)
    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle1')
    loader('example/aBundle')

    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle2')
    loader('example/aBundle')
    index_requests = []
    requests = http_bundle_server.requests
    while not requests.empty():
        req = requests.get()
        if req['path'] == '/index.json':
            index_requests.append(req)
    assert len(index_requests) == 1
