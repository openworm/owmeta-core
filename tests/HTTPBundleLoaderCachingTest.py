from os.path import join as p, isfile

import pytest
import requests

from owmeta_core.bundle.loaders.http import HTTPBundleLoader, HTTPURLConfig


@pytest.mark.inttest
def test_directory_cache_index_etag(http_bundle_server, tempdir):
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


@pytest.mark.inttest
def test_mem_cache_index_etag(http_bundle_server, tempdir):
    config = HTTPURLConfig(f'{http_bundle_server.url}/index.json',
            mem_cache=True)
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


@pytest.mark.inttest
def test_session_provider(http_bundle_server, tempdir):
    '''
    Test a custom session provider. We add a header to the session since that's easy to
    test and it's one thing that you can't do conveniently without this feature
    '''
    config = HTTPURLConfig(f'{http_bundle_server.url}/index.json',
            session_provider=f'{__name__}:session_provider')
    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle')
    loader('example/aBundle')

    reqs = http_bundle_server.requests
    while not reqs.empty():
        req = reqs.get()
        assert 'hello-i-am-header' in req['headers']


def session_provider():
    sess = requests.Session()
    sess.headers = {'hello-i-am-header': 'nice to meet you'}
    return sess


@pytest.mark.inttest
def test_session_persistence_cookies(http_bundle_server, tempdir):
    '''
    Test that we load cookies from a persisted session
    '''

    def headers(req):
        return {'Set-Cookie': 'jamba=laya'}

    http_bundle_server.headers = headers
    http_bundle_server.restart()

    config = HTTPURLConfig(f'{http_bundle_server.url}/index.json', session_file_name=p(tempdir, 'sessfile'))
    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle1')
    loader('example/aBundle')

    config = HTTPURLConfig(f'{http_bundle_server.url}/index.json', session_file_name=p(tempdir, 'sessfile'))
    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle2')
    loader('example/aBundle')

    reqs = http_bundle_server.requests
    skip = True
    rcount = 0
    while not reqs.empty():
        rcount += 1
        if skip:
            reqs.get()
            skip = False
            continue
        headers = reqs.get()['headers']
        assert headers.get('cookie') == 'jamba=laya', f'request count {rcount}'


@pytest.mark.owm_cli_test
def test_expand_tilde_session_file_name(http_bundle_server, owm_project):
    '''
    Test setting the session_file_name with a tilde in it.

    Good for sharing remotes in a project
    '''
    session_file_name = p("~", "sessfile")
    owm_project.sh(f'owm bundle remote add test_remote {http_bundle_server.url}/index.json'
            f' http --session-file-name {session_file_name}')
    owm_project.sh('owm bundle fetch example/aBundle')
    assert isfile(p(owm_project.test_homedir, 'sessfile'))

# command tests
# - test creating with a cache dir
# - test creating with a mem cache
# - test session function with a cache dir


@pytest.mark.inttest
def test_cache_bundle_etag(http_bundle_server, tempdir):
    cache_dir = p(tempdir, 'cache')
    config = HTTPURLConfig(f'{http_bundle_server.url}/index.json',
            cache_dir=cache_dir)
    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle1')
    loader('example/aBundle')

    loader = HTTPBundleLoader(config)
    loader.base_directory = p(tempdir, 'bundle2')
    loader('example/aBundle')
    bundle_requests = []
    requests = http_bundle_server.requests
    while not requests.empty():
        req = requests.get()
        if req['path'] != '/index.json':
            # Anything that isn't the index should be the bundle
            bundle_requests.append(req)
    assert len(bundle_requests) == 1
