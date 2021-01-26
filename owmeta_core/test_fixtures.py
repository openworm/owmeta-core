from collections import namedtuple
import shutil

from pytest import fixture

from .bundle import (find_bundle_directory, AccessorConfig, Remote)
from .bundle.loaders import Loader


BundleData = namedtuple('BundleData', ('id', 'version', 'source_directory', 'remote'))


def bundle_fixture_helper(bundle_id):
    def bundle(request):
        for version_mark in request.node.iter_markers('bundle_version'):
            mark_id = version_mark.args[0]
            if bundle_id == mark_id:
                version = version_mark.args[1]
                break
        else: # no break
            raise Exception('Must specify a version of the bundle')

        source_directory = find_bundle_directory('bundles', bundle_id, version)

        class TestAC(AccessorConfig):
            def __eq__(self, other):
                return other is self

            def __hash__(self):
                return object.__hash__(self)

        class TestBundleLoader(Loader):
            def __init__(self, ac):
                pass

            def bundle_versions(self):
                return [1]

            @classmethod
            def can_load_from(cls, ac):
                if isinstance(ac, TestAC):
                    return True
                return False

            def can_load(self, ident, version): return True

            def load(self, ident, version):
                shutil.copytree(source_directory, self.base_directory)

        TestBundleLoader.register()
        remote = Remote('test', (TestAC(),))

        yield BundleData(
                bundle_id,
                version,
                source_directory,
                remote)
    return bundle


core_bundle = fixture(bundle_fixture_helper('openworm/owmeta-core'))


def pytest_configure(config):
    config.addinivalue_line("markers",
            "bundle_version(bundle_id, bundle_version): Marks for telling bundle fixtures"
            " their version number")
