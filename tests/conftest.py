from contextlib import contextmanager
from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
from multiprocessing import Process, Queue
from subprocess import check_output, CalledProcessError
from os import chdir
import os
from os.path import join as p
from textwrap import dedent
import shutil
import shlex
import tempfile

import requests
from pytest import fixture


L = logging.getLogger(__name__)


@fixture
def tempdir():
    with tempfile.TemporaryDirectory(prefix=__name__ + '.') as td:
        yield td


class ServerData():
    def __init__(self, server, request_queue):
        self.server = server
        self.requests = request_queue
        self.scheme = 'http'

    @property
    def url(self):
        return self.scheme + '://{}:{}'.format(*self.server.server_address)


@contextmanager
def _http_server():
    srvdir = tempfile.mkdtemp(prefix=__name__ + '.')
    process = None
    request_queue = Queue()
    try:
        server = make_server(request_queue)

        def pfunc():
            chdir(srvdir)
            server.serve_forever()

        process = Process(target=pfunc)

        server_data = ServerData(server, request_queue)

        def start():
            process.start()
            wait_for_started(server_data)

        server_data.start = start
        yield server_data
    finally:
        if process:
            process.terminate()
            process.join()
        shutil.rmtree(srvdir)


@fixture
def https_server():
    import ssl
    with _http_server() as server_data:
        server_data.server.socket = \
                ssl.wrap_socket(server_data.server.socket,
                        certfile=p('tests', 'cert.pem'),
                        keyfile=p('tests', 'key.pem'),
                        server_side=True)
        server_data.start()
        server_data.ssl_context = ssl.SSLContext()
        server_data.ssl_context.load_verify_locations(p('tests', 'cert.pem'))
        server_data.scheme = 'https'
        yield server_data


@fixture
def http_server():
    with _http_server() as server_data:
        server_data.start()
        yield server_data


def make_server(request_queue):
    class _Handler(SimpleHTTPRequestHandler):
        def handle_request(self, code):
            request_queue.put(dict(
                method=self.command,
                path=self.path,
                headers={k.lower(): v for k, v in self.headers.items()}))
            self.send_response(code)
            self.end_headers()

        def do_POST(self):
            self.handle_request(201)

    port = 8000
    while True:
        try:
            server = HTTPServer(('127.0.0.1', port), _Handler)
            break
        except OSError as e:
            if e.errno != 98:
                raise
            port += 1

    return server


def wait_for_started(server_data, max_tries=10):
    done = False
    tries = 0
    while not done and tries < max_tries:
        tries += 1
        try:
            requests.head(server_data.url)
            done = True
        except Exception:
            L.info("Unable to connect to the bundle server. Trying again.", exc_info=True)


@fixture
def owm_project():
    res = Data()
    res.testdir = tempfile.mkdtemp(prefix=__name__ + '.')
    res.test_homedir = p(res.testdir, 'homedir')
    os.mkdir(res.test_homedir)
    with open(p('tests', 'pytest-cov-embed.py'), 'r') as f:
        ptcov = f.read()
    # Added so pytest_cov gets to run for our subprocesses
    with open(p(res.testdir, 'sitecustomize.py'), 'w') as f:
        f.write(ptcov)

    try:
        res.sh('owm -b init --default_context_id "http://example.org/data"')
        yield res
    finally:
        shutil.rmtree(res.testdir)


class Data(object):
    exception = None

    def __str__(self):
        items = []
        for m in vars(self):
            if (m.startswith('_') or m == 'sh'):
                continue
            items.append(m + '=' + repr(getattr(self, m)))
        return 'Data({})'.format(', '.join(items))

    def writefile(self, name, contents):
        with open(p(self.testdir, name), 'w') as f:
            print(dedent(contents), file=f)
            f.flush()

    def sh(self, *command, **kwargs):
        if not command:
            return None
        env = dict(os.environ)
        env['PYTHONPATH'] = self.testdir + os.pathsep + env['PYTHONPATH']
        env['HOME'] = self.test_homedir
        env.update(kwargs.pop('env', {}))
        outputs = []
        for cmd in command:
            try:
                outputs.append(check_output(shlex.split(cmd), env=env, cwd=self.testdir, **kwargs).decode('utf-8'))
            except CalledProcessError as e:
                if e.output:
                    print(dedent('''\
                    ----------stdout from "{}"----------
                    {}
                    ----------{}----------
                    ''').format(cmd, e.output.decode('UTF-8'),
                               'end stdout'.center(14 + len(cmd))))
                if getattr(e, 'stderr', None):
                    print(dedent('''\
                    ----------stderr from "{}"----------
                    {}
                    ----------{}----------
                    ''').format(cmd, e.stderr.decode('UTF-8'),
                               'end stderr'.center(14 + len(cmd))))
                raise
        return outputs[0] if len(outputs) == 1 else outputs

    __repr__ = __str__
