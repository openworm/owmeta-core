import os
from os.path import join as p
import io
import json
import tempfile
import shutil
import tarfile

import rdflib
from rdflib import ConjunctiveGraph, URIRef
from pytest import fixture, raises
from unittest.mock import patch, Mock

from owmeta_core.bundle import (NoRemoteAvailable,
                                Remote,
                                Deployer,
                                NotABundlePath)


@fixture
def remote():
    uploader = Mock()
    rem = Mock(spec=Remote)
    rem.name = 'remote'
    rem.generate_uploaders.return_value = [uploader]
    return rem


def test_bundle_path_does_not_exist(tempdir):
    ''' Can't deploy a bundle we don't have '''
    cut = Deployer()
    with raises(NotABundlePath):
        cut.deploy(p(tempdir, 'notabundle'))


def test_bundle_directory_lacks_manifest(tempdir):
    ''' A valid bundle needs a manifest '''

    cut = Deployer()
    os.mkdir(p(tempdir, 'notabundle'))
    with raises(NotABundlePath):
        cut.deploy(p(tempdir, 'notabundle'))


def test_bundle_directory_manifest_is_a_directory(tempdir):
    ''' A valid bundle manifest is not a directory '''

    cut = Deployer()
    os.makedirs(p(tempdir, 'notabundle', 'manifest'))
    with raises(NotABundlePath):
        cut.deploy(p(tempdir, 'notabundle'))


def test_bundle_directory_manifest_has_no_version(tempdir):
    '''
    A valid bundle manifest has a version number, up to a specific version, all other
    fields are optional
    '''
    cut = Deployer()
    bdir = p(tempdir, 'notabundle')
    os.makedirs(bdir)
    with open(p(bdir, 'manifest'), 'w') as mf:
        json.dump({}, mf)
    with raises(NotABundlePath):
        cut.deploy(bdir)


def test_bundle_directory_manifest_empty(tempdir):
    '''
    An empty file is not a valid manifest
    '''
    cut = Deployer()
    bdir = p(tempdir, 'notabundle')
    os.makedirs(bdir)
    open(p(bdir, 'manifest'), 'w').close()
    with raises(NotABundlePath):
        cut.deploy(bdir)


def test_bundle_directory_manifest_has_unknown_manifest_version(tempdir):
    cut = Deployer()
    bdir = p(tempdir, 'notabundle')
    os.makedirs(bdir)
    with open(p(bdir, 'manifest'), 'w') as mf:
        json.dump({'manifest_version': 2}, mf)
    with raises(NotABundlePath):
        cut.deploy(bdir)


def test_bundle_directory_manifest_has_no_bundle_version(tempdir):
    cut = Deployer()
    bdir = p(tempdir, 'notabundle')
    os.makedirs(bdir)
    with open(p(bdir, 'manifest'), 'w') as mf:
        json.dump({'manifest_version': 1}, mf)
    with raises(NotABundlePath):
        cut.deploy(bdir)


def test_bundle_directory_manifest_has_no_bundle_id(tempdir):
    cut = Deployer()
    bdir = p(tempdir, 'notabundle')
    os.makedirs(bdir)
    with open(p(bdir, 'manifest'), 'w') as mf:
        json.dump({'manifest_version': 1, 'version': 1}, mf)
    with raises(NotABundlePath):
        cut.deploy(bdir)


def test_deploy_directory_from_installer(bundle, remote):
    ''' Test that we can deploy an installed bundle '''
    Deployer().deploy(
        bundle.bundle_directory,
        remotes=(remote,)
    )


def test_deploy_directory_no_remotes(bundle):
    ''' We can't deploy if we don't have any remotes '''
    with raises(NoRemoteAvailable):
        Deployer().deploy(bundle.bundle_directory)


def test_deploy_archive_no_remotes(bundle_archive):
    '''
    Test deploying an archive
    '''
    cut = Deployer()
    with raises(NoRemoteAvailable):
        cut.deploy(bundle_archive.archive_path)


def test_deploy_archive_validate_manifest(bundle_archive, remote):
    '''
    Test manifest validation
    '''
    cut = Deployer()
    with patch('owmeta_core.bundle.validate_manifest') as vm:
        cut.deploy(bundle_archive.archive_path, remotes=(remote,))
        vm.assert_called()


def test_deploy_archive_no_manifest_not_a_bundle(tempdir):
    '''
    Test missing manifest
    '''
    bundle_path = p(tempdir, 'bundle.tar.xz')
    with tarfile.open(bundle_path, 'w:xz') as tf:
        pass

    rem = Remote('remote')
    cut = Deployer()
    with raises(NotABundlePath):
        cut.deploy(bundle_path, remotes=(rem,))


def test_deploy_archive_manifest_isdir_not_a_bundle(tempdir):
    '''
    Test missing manifest
    '''
    bundle_path = p(tempdir, 'bundle.tar.xz')
    with tarfile.open(bundle_path, 'w:xz') as tf:
        tinfo = tarfile.TarInfo('manifest')
        tinfo.type = tarfile.DIRTYPE
        tf.addfile(tinfo)

    rem = Remote('remote')
    cut = Deployer()
    with raises(NotABundlePath):
        cut.deploy(bundle_path, remotes=(rem,))


def test_deploy_archive_manifest_emptyfile_not_a_bundle(tempdir):
    '''
    Test manifest empty/malformed
    '''
    bundle_path = p(tempdir, 'bundle.tar.xz')
    with tarfile.open(bundle_path, 'w:xz') as tf:
        tinfo = tarfile.TarInfo('manifest')
        tf.addfile(tinfo)

    rem = Remote('remote')
    cut = Deployer()
    with raises(NotABundlePath):
        cut.deploy(bundle_path, remotes=(rem,))
