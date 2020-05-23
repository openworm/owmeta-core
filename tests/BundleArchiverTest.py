import json
import os
from os.path import exists, join as p
import tarfile

import pytest

from owmeta_core.bundle.archive import (Archiver, ArchiveTargetPathDoesNotExist)
from owmeta_core.bundle.exceptions import BundleNotFound
from owmeta_core.bundle import Descriptor, BUNDLE_INDEXED_DB_NAME, BUNDLE_MANIFEST_FILE_NAME


def test_archive_returns_non_none(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(bundle.descriptor.id, bundle.descriptor.version)
    assert s is not None


def test_archive_exists(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(bundle.descriptor.id, bundle.descriptor.version)
    assert exists(s)


def test_latest_bundle_selected_by_default(tempdir, custom_bundle):
    d1desc = Descriptor.load('''
    id: dep
    version: 1
    ''')
    d2desc = Descriptor.load('''
    id: dep
    version: 2
    ''')
    bdir = p(tempdir, 'bundles')
    with custom_bundle(d1desc, bundles_directory=bdir), \
            custom_bundle(d2desc, bundles_directory=bdir):
        print('dir contents', os.listdir(bdir))
        s = Archiver(tempdir, bdir).pack('dep')
        with tarfile.open(s, 'r:xz') as tf:
            with tf.extractfile(BUNDLE_MANIFEST_FILE_NAME) as mf:
                md = json.load(mf)
                assert md.get('version') == 2


def test_archive_writen_to_target_file_relative(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(
            bundle.descriptor.id, bundle.descriptor.version,
            target_file_name='targetfilename')
    assert exists(p(tempdir, 'targetfilename'))


def test_archive_writen_to_target_file_absolute(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(
            bundle.descriptor.id, bundle.descriptor.version,
            target_file_name=p(tempdir, 'targetfilename'))
    assert exists(p(tempdir, 'targetfilename'))


def test_archive_writen_to_target_path_absolute_does_not_exist(tempdir, bundle):
    with pytest.raises(ArchiveTargetPathDoesNotExist):
        s = Archiver(tempdir, bundle.bundles_directory).pack(
                bundle.descriptor.id, bundle.descriptor.version,
                target_file_name=p(tempdir, 'somedir', 'targetfilename'))


def test_archive_writen_to_target_path_relative_does_not_exist(tempdir, bundle):
    with pytest.raises(ArchiveTargetPathDoesNotExist):
        s = Archiver(tempdir, bundle.bundles_directory).pack(
                bundle.descriptor.id, bundle.descriptor.version,
                target_file_name=p('somedir', 'targetfilename'))


def test_archive_omits_indexed_db(tempdir, bundle):
    if not exists(p(bundle.bundle_directory, BUNDLE_INDEXED_DB_NAME)):
        pytest.fail('Invalid test -- "%s" does not exist to start with' %
                BUNDLE_INDEXED_DB_NAME)

    s = Archiver(tempdir, bundle.bundles_directory).pack(
                bundle.descriptor.id, bundle.descriptor.version,
                target_file_name='targetfilename')

    with tarfile.open(s, 'r:xz') as tf:
        for mem in tf.getmembers():
            if mem.name.startswith(BUNDLE_INDEXED_DB_NAME):
                pytest.fail("Should not have included the %s in the archive" %
                        mem.name)


def test_archive_nonexistant_bundle_throws_BNF(tempdir):
    with pytest.raises(BundleNotFound):
        Archiver(tempdir, tempdir).pack('bundle_id', 1)


def test_archive_nonexistant_bundle_version_throws_BNF(tempdir, bundle):
    with pytest.raises(BundleNotFound):
        Archiver(tempdir, bundle.bundles_directory).pack(bundle.descriptor.id, bundle.descriptor.version + 1)


def test_bundle_directory_path_returns_non_none(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(bundle_directory=bundle.bundle_directory)
    assert s is not None
