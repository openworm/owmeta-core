from os.path import exists, join as p
import tarfile

import pytest

from owmeta_core.bundle import (Archiver, BundleNotFound, ArchiveTargetPathDoesNotExist,
        BUNDLE_INDEXED_DB_NAME)


def test_archive_returns_string(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(bundle.descriptor.id, bundle.descriptor.version)
    assert s is not None


def test_archive_exists(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(bundle.descriptor.id, bundle.descriptor.version)
    assert exists(s)


def test_archive_writen_to_target_file_relative(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(
            bundle.descriptor.id, bundle.descriptor.version, 'targetfilename')
    assert exists(p(tempdir, 'targetfilename'))


def test_archive_writen_to_target_file_absolute(tempdir, bundle):
    s = Archiver(tempdir, bundle.bundles_directory).pack(
            bundle.descriptor.id, bundle.descriptor.version, p(tempdir, 'targetfilename'))
    assert exists(p(tempdir, 'targetfilename'))


def test_archive_writen_to_target_path_absolute_does_not_exist(tempdir, bundle):
    with pytest.raises(ArchiveTargetPathDoesNotExist):
        s = Archiver(tempdir, bundle.bundles_directory).pack(
                bundle.descriptor.id, bundle.descriptor.version, p(tempdir, 'somedir', 'targetfilename'))


def test_archive_writen_to_target_path_relative_does_not_exist(tempdir, bundle):
    with pytest.raises(ArchiveTargetPathDoesNotExist):
        s = Archiver(tempdir, bundle.bundles_directory).pack(
                bundle.descriptor.id, bundle.descriptor.version, p('somedir', 'targetfilename'))


def test_archive_omits_indexed_db(tempdir, bundle):
    if not exists(p(bundle.bundle_directory, BUNDLE_INDEXED_DB_NAME)):
        pytest.fail('Invalid test -- "%s" does not exist to start with' %
                BUNDLE_INDEXED_DB_NAME)

    s = Archiver(tempdir, bundle.bundles_directory).pack(
                bundle.descriptor.id, bundle.descriptor.version, 'targetfilename')

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
