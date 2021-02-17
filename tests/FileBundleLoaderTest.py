'''
Note that we don't test that the base directory *exists* in the loader *until* we check if
we can load a specific bundle. This is to give maximum freedom in ordering setup of these
things and is consistent with how other components are designed in owmeta_core.
'''

from pathlib import Path
from unittest.mock import patch

import pytest

from owmeta_core.bundle.common import BundleNotFound
from owmeta_core.bundle.loaders import LoadFailed
from owmeta_core.bundle.loaders.local import FileBundleLoader, FileURLConfig


def test_file_bundle_loader_can_load_from():
    fc = FileURLConfig('file:///this/doesnt/matter')
    assert FileBundleLoader.can_load_from(fc)


@pytest.mark.inttest
def test_file_bundle_loader_can_load_fail_not_exists(tmpdir):
    fc = FileURLConfig(f'file://{tmpdir}/does/not/exist')
    loader = FileBundleLoader(fc)
    assert not loader.can_load('something', 3939)


def test_file_bundle_loader_can_load_success():
    fc = FileURLConfig('file:///this/does/not/matter')
    loader = FileBundleLoader(fc)
    with patch('owmeta_core.bundle.loaders.local.find_bundle_directory'):
        assert loader.can_load('irrelevant', 200)


def test_file_bundle_loader_can_load_fail_BundleNotFound():
    fc = FileURLConfig('file:///this/does/not/matter')
    loader = FileBundleLoader(fc)
    with patch('owmeta_core.bundle.loaders.local.find_bundle_directory') as fbdir:
        fbdir.side_effect = BundleNotFound('blah')
        assert not loader.can_load('irrelevant', 404)


def test_file_bundle_loader_init_fail_type():
    with pytest.raises(TypeError):
        FileBundleLoader(123245)


def test_file_bundle_loader_init_fail_not_absolute_str():
    with pytest.raises(ValueError):
        FileBundleLoader('not_abs')


def test_file_bundle_loader_init_fail_not_absolute_path():
    with pytest.raises(ValueError):
        FileBundleLoader(Path('not_abs'))


def test_file_bundle_loader_init_fail_not_absolute_url():
    with pytest.raises(ValueError):
        FileBundleLoader(FileURLConfig('file://not_abs'))


def test_file_bundle_loader_load_fail_source_not_found():
    fc = FileURLConfig('file:///this/does/not/matter')
    loader = FileBundleLoader(fc)
    with patch('owmeta_core.bundle.loaders.local.find_bundle_directory') as fbdir:
        fbdir.side_effect = BundleNotFound('blah')
        with pytest.raises(LoadFailed, match=r'source directory'):
            loader.load('irrelevant', 404)


def test_file_bundle_loader_load_fail_source_not_found_no_version():
    fc = FileURLConfig('file:///this/does/not/matter')
    loader = FileBundleLoader(fc)
    with patch('owmeta_core.bundle.loaders.local.find_bundle_directory') as fbdir:
        fbdir.side_effect = BundleNotFound('blah')
        with pytest.raises(LoadFailed, match=r'source directory'):
            loader.load('irrelevant')


def test_file_bundle_loader_load_fail_copytree():
    fc = FileURLConfig('file:///this/does/not/matter')
    loader = FileBundleLoader(fc)
    with patch('owmeta_core.bundle.loaders.local.find_bundle_directory') as fbdir, \
            patch('owmeta_core.bundle.loaders.local.BundleTreeFileIgnorer') as ignore, \
            patch('owmeta_core.bundle.loaders.local.shutil') as shutil:
        fbdir.return_value = 'source_dir'
        ignore().return_value = []
        shutil.copytree.side_effect = ValueError
        with pytest.raises(LoadFailed, match=r'copy'):
            loader.load('irrelevant', 404)


@pytest.mark.inttest
def test_file_bundle_loader_load(tmpdir, test_bundle):
    fc = FileURLConfig(f'file://{test_bundle.bundles_directory}')
    loader = FileBundleLoader(fc)
    descr = test_bundle.descriptor
    loader.base_directory = Path(tmpdir, 'target_dir')
    loader.load(descr.id, descr.version)
