from __future__ import print_function
from os.path import join as p, exists
from os import makedirs
import shutil
import subprocess
import tarfile

import pytest
from pytest import mark
from rdflib.term import URIRef, Literal
import transaction

from owmeta_core.command import DEFAULT_OWM_DIR as OD, OWM
from owmeta_core.bundle import Descriptor


from .TestUtilities import assertRegexpMatches, assertNotRegexpMatches


pytestmark = mark.owm_cli_test


def test_load(owm_project):
    owm_bundle = p('tests', 'test_data', 'example_bundle.tar.xz')
    target_bundle = p(owm_project.testdir, 'bundle.tar.xz')
    shutil.copyfile(owm_bundle, target_bundle)
    owm_project.sh('owm bundle load ' + target_bundle)
    assertRegexpMatches(
        owm_project.sh('owm bundle cache list'),
        r'example/aBundle@23'
    )


def add_bundle(owm_project):
    owm_project.writefile('abundle.yml', '''\
    ---
    id: abundle
    description: I'm a description
    includes: ["http://example.org/test_ctx"]
    ''')
    with OWM(owmdir=p(owm_project.testdir, OD)).connect() as conn:
        with transaction.manager:
            graph = conn.conf['rdf.graph']
            sg = graph.get_context('http://example.org/test_ctx')
            sg.add((URIRef('http://example.org/a'),
                    URIRef('http://example.org/b'),
                    Literal('c')))

    homedir = p(owm_project.testdir, 'home')
    makedirs(homedir)
    owm_project.homedir = homedir
    owm_project.sh('owm bundle register abundle.yml')


def test_install(owm_project):
    '''
    Install a bundle and make sure we can use it with Bundle
    '''
    add_bundle(owm_project)
    print(owm_project.sh('owm bundle install abundle',
        env={'HOME': owm_project.homedir}))
    owm_project.writefile('use.py', '''\
    from owmeta_core.bundle import Bundle
    from rdflib.term import URIRef, Literal
    with Bundle('abundle') as bnd:
        # "contextualize" the Context with the bundle to access contexts within the bundle
        print((URIRef('http://example.org/a'),
               URIRef('http://example.org/b'),
               Literal('c')) in bnd.rdf, end='')
    ''')
    assert owm_project.sh('python use.py', env={'HOME': owm_project.homedir}) == 'True'


def test_install_prompt_delete_when_target_directory_not_empty(owm_project):
    add_bundle(owm_project)
    # Install once
    bundle_directory = owm_project.sh('owm bundle install abundle',
            env={'HOME': owm_project.homedir}).strip()
    if not bundle_directory:
        pytest.fail("Bundle directory not provided in install output")
    # Place a marker in the bundle directory
    marker = p(bundle_directory, 'marker')
    open(marker, 'w').close()

    # Attempt another install. Should fail and prompt to overwrite
    fname = p(owm_project.testdir, 'input')
    with open(fname, 'w') as inp:
        inp.write('yes\n')

    with open(fname, 'r') as inp:
        owm_project.sh('owm bundle install abundle', stdin=inp, env={'HOME': owm_project.homedir})
    assert not exists(marker)


def test_install_prompt_keep_when_target_directory_not_empty(owm_project):
    add_bundle(owm_project)
    # Install once
    bundle_directory = owm_project.sh('owm bundle install abundle',
            env={'HOME': owm_project.homedir}).strip()
    if not bundle_directory:
        pytest.fail("Bundle directory not provided in install output")
    # Place a marker in the bundle directory
    marker = p(bundle_directory, 'marker')
    open(marker, 'w').close()

    # Attempt another install. Should fail and prompt to overwrite
    fname = p(owm_project.testdir, 'input')
    with open(fname, 'w') as inp:
        inp.write('no\n')

    with open(fname, 'r') as inp:
        owm_project.sh('owm bundle install abundle', stdin=inp, env={'HOME': owm_project.homedir})
    assert exists(marker)


def test_non_interactive_install_fail_when_target_directory_not_empty(owm_project):
    add_bundle(owm_project)
    # Install once
    bundle_directory = owm_project.sh('owm bundle install abundle',
            env={'HOME': owm_project.homedir}).strip()
    if not bundle_directory:
        pytest.fail("Bundle directory not provided in install output")
    # Place a marker in the bundle directory
    marker = p(bundle_directory, 'marker')
    open(marker, 'w').close()

    # Attempt another install. Should fail
    with pytest.raises(subprocess.CalledProcessError):
        owm_project.sh('owm -b bundle install abundle', env={'HOME': owm_project.homedir})
    assert exists(marker)


def test_register(owm_project):
    owm_project.writefile('abundle.yml', '''\
    ---
    id: abundle
    description: I'm a description
    ''')
    owm_project.sh('owm bundle register abundle.yml')
    assertRegexpMatches(
        owm_project.sh('owm bundle list'),
        r'abundle - I\'m a description'
    )


def test_list_descriptor_removed(owm_project):
    owm_project.writefile('abundle.yml', '''\
    ---
    id: abundle
    description: I'm a description
    ''')
    owm_project.sh('owm bundle register abundle.yml',
            'rm abundle.yml')
    assertRegexpMatches(
        owm_project.sh('owm bundle list'),
        r"abundle - ERROR: Cannot read bundle descriptor at 'abundle.yml'"
    )


def test_list_descriptor_moved(owm_project):
    owm_project.writefile('abundle.yml', '''\
    ---
    id: abundle
    description: I'm a description
    ''')
    owm_project.sh('owm bundle register abundle.yml',
            'mv abundle.yml bundle.yml')
    assertRegexpMatches(
        owm_project.sh('owm bundle list'),
        r"abundle - ERROR: Cannot read bundle descriptor at 'abundle.yml'"
    )


def test_reregister(owm_project):
    owm_project.writefile('abundle.yml', '''\
    ---
    id: abundle
    description: I'm a description
    ''')
    owm_project.sh('owm bundle register abundle.yml',
            'mv abundle.yml bundle.yml',
            'owm bundle register bundle.yml')
    assertRegexpMatches(
        owm_project.sh('owm bundle list'),
        r"abundle - I'm a description"
    )


def test_reregister_new_id(owm_project):
    owm_project.writefile('abundle.yml', '''\
    ---
    id: abundle
    description: I'm a description
    ''')
    owm_project.sh('owm bundle register abundle.yml')
    owm_project.writefile('abundle.yml', '''\
    ---
    id: bubble
    description: I'm a description
    ''')
    owm_project.sh('owm bundle register abundle.yml')
    assertNotRegexpMatches(
        owm_project.sh('owm bundle list'),
        r"abundle"
    )


def test_cache_list(shell_helper):
    '''
    List bundles in the cache
    '''
    bundle_dir = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '1')
    makedirs(bundle_dir)
    with open(p(bundle_dir, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main"}')
    assertRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'test/main@1'
    )


def test_cache_list_empty(shell_helper):
    '''
    List bundles in the cache
    '''
    assert shell_helper.sh('owm bundle cache list') == ''


def test_cache_list_multiple_versions(shell_helper):
    '''
    List bundles in the cache.

    For the same bundle ID, they should be in reverse version order (newest versions
    first)
    '''
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '1')
    bundle_dir2 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '2')
    makedirs(bundle_dir1)
    makedirs(bundle_dir2)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main"}')
    with open(p(bundle_dir2, 'manifest'), 'w') as mf:
        mf.write('{"version": 2, "id": "test/main"}')
    assertRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'test/main@2\ntest/main@1'
    )


def test_cache_list_different_bundles(shell_helper):
    '''
    List bundles in the cache
    '''
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '1')
    bundle_dir2 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fsecondary', '1')
    makedirs(bundle_dir1)
    makedirs(bundle_dir2)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main"}')
    with open(p(bundle_dir2, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/secondary"}')
    assertRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'test/main@1'
    )
    assertRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'test/secondary@1'
    )


def test_cache_list_version_check(shell_helper):
    '''
    bundle cache list filters out bundles with the wrong version
    '''
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '1')
    bundle_dir2 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fsecondary', '2')
    makedirs(bundle_dir1)
    makedirs(bundle_dir2)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main"}')
    with open(p(bundle_dir2, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/secondary"}')
    assertRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'test/main@1'
    )
    assertNotRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'test/secondary@1'
    )


def test_cache_list_version_check_warning(shell_helper):
    '''
    bundle cache list filters out bundles with the wrong version
    '''
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '1')
    bundle_dir2 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fsecondary', '2')
    makedirs(bundle_dir1)
    makedirs(bundle_dir2)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main"}')
    with open(p(bundle_dir2, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/secondary"}')
    output = shell_helper.sh('owm bundle cache list', stderr=subprocess.STDOUT)
    assertRegexpMatches(output, r'manifest.*match')


def test_cache_list_description(shell_helper):
    '''
    Make sure the bundle description shows up
    '''
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
                   'test%2Fmain', '1')
    makedirs(bundle_dir1)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main", "description": "Waka waka"}')
    assertRegexpMatches(
        shell_helper.sh('owm bundle cache list'),
        r'Waka waka'
    )


def test_save_creates_file(shell_helper):
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
            'test%2Fmain', '1')
    makedirs(bundle_dir1)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main", "description": "Waka waka"}')
    shell_helper.sh('owm bundle save test/main test-main.tar.xz')
    assert exists(p(shell_helper.testdir, 'test-main.tar.xz'))


def test_save_is_archive(shell_helper):
    bundle_dir1 = p(shell_helper.test_homedir, '.owmeta', 'bundles',
            'test%2Fmain', '1')
    makedirs(bundle_dir1)
    with open(p(bundle_dir1, 'manifest'), 'w') as mf:
        mf.write('{"version": 1, "id": "test/main", "description": "Waka waka"}')
    shell_helper.sh('owm bundle save test/main test-main.tar.xz')
    assert tarfile.is_tarfile(p(shell_helper.testdir, 'test-main.tar.xz'))


@mark.sftp
def test_deploy_sftp(owm_project_with_customizations, custom_bundle):
    desc = Descriptor('test/main', includes=('http://example.org/ctx',))
    with owm_project_with_customizations(customizations='''\
            from unittest.mock import patch
            import atexit
            patch('owmeta_core.bundle.loaders.sftp.Transport').start()
            SFTPClientPatcher = patch('owmeta_core.bundle.loaders.sftp.SFTPClient')
            SFTPClient = SFTPClientPatcher.start()
            def verify():
                try:
                    SFTPClient.from_transport().__enter__().put.assert_called()
                except AssertionError:
                    print("FAILED")
            atexit.register(verify)
            ''') as owm_project:
        with custom_bundle(desc, bundles_directory=p(owm_project.test_homedir, '.owmeta', 'bundles')):
            owm_project.sh('owm bundle remote add the-source sftp://example.org/this/doesnt/matter')
            owm_project.apply_customizations()
            output = owm_project.sh('owm bundle deploy test/main')
            assert 'FAILED' not in output


def test_checkout(owm_project):
    '''
    Checking out a bundle changes the set of graphs to the chosen bundle
    '''
    owm_project.sh('owm bundle checkout test/main')
    # TODO: Add an assert

# TODO: Test for bundles with extras that aren't installed
# TODO: Test for bundle remotes that depend on extras that aren't installed
