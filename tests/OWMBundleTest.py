from __future__ import print_function
from os.path import join as p, exists
from os import makedirs
import shutil
import subprocess
from subprocess import CalledProcessError
import tarfile

import pytest
from pytest import mark
import rdflib
from rdflib.term import URIRef, Literal
import transaction

from owmeta_core.context import Context
from owmeta_core.command import DEFAULT_OWM_DIR as OD, OWM
from owmeta_core.bundle import DependencyDescriptor, Descriptor, Bundle, make_include_func
from owmeta_pytest_plugin import bundle_versions

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


def add_bundle(owm_project, descriptor=None):
    owm_project.writefile('abundle.yml', descriptor or '''\
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

    owm_project.sh('owm bundle register abundle.yml')


def test_install(owm_project):
    '''
    Install a bundle and make sure we can use it with Bundle
    '''
    add_bundle(owm_project)
    print(owm_project.sh('owm bundle install abundle'))
    owm_project.writefile('use.py', '''\
    from owmeta_core.bundle import Bundle
    from rdflib.term import URIRef, Literal
    with Bundle('abundle') as bnd:
        # "contextualize" the Context with the bundle to access contexts within the bundle
        print((URIRef('http://example.org/a'),
               URIRef('http://example.org/b'),
               Literal('c')) in bnd.rdf, end='')
    ''')
    assert owm_project.sh('python use.py') == 'True'


def test_install_prompt_delete_when_target_directory_not_empty(owm_project):
    add_bundle(owm_project)
    # Install once
    bundle_directory = owm_project.sh('owm bundle install abundle').strip()
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
        owm_project.sh('owm bundle install abundle', stdin=inp)
    assert not exists(marker)


def test_install_prompt_keep_when_target_directory_not_empty(owm_project):
    add_bundle(owm_project)
    # Install once
    bundle_directory = owm_project.sh('owm bundle install abundle').strip()
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
        owm_project.sh('owm bundle install abundle', stdin=inp)
    assert exists(marker)


def test_non_interactive_install_fail_when_target_directory_not_empty(owm_project):
    add_bundle(owm_project)
    # Install once
    bundle_directory = owm_project.sh('owm bundle install abundle').strip()
    if not bundle_directory:
        pytest.fail("Bundle directory not provided in install output")
    # Place a marker in the bundle directory
    marker = p(bundle_directory, 'marker')
    open(marker, 'w').close()

    # Attempt another install. Should fail
    with pytest.raises(CalledProcessError):
        owm_project.sh('owm -b bundle install abundle')
    assert exists(marker)


@bundle_versions('core_bundle', [1, 2])
def test_install_class_registry_load(owm_project, core_bundle):
    owm_project.fetch(core_bundle)
    from tests.test_modules.owmbundletest01 import Person

    modpath = p('tests', 'test_modules')
    owm_project.make_module(modpath)
    owm_project.writefile(p(modpath, 'owmbundletest02_defs.py'))
    owm_project.copy(p(modpath, 'owmbundletest02_query.py'), 'query.py')
    save_output = owm_project.sh('owm save tests.test_modules.owmbundletest02_defs')
    print("---------vSAVE OUTPUTv----------")
    print(save_output)
    print("---------^SAVE OUTPUT^----------")
    descriptor = f'''\
    ---
    id: person_bundle
    description: A person in a bundle
    includes: ["{owm_project.default_context_id}", "{Person.definition_context.identifier}"]
    dependencies:
        - id: openworm/owmeta-core
          version: 1
    '''
    print(descriptor)
    add_bundle(owm_project, descriptor)
    owm_project.sh('owm bundle install person_bundle')
    owm_project.sh('python query.py')


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


def test_load_from_class_registry_from_conjunctive(custom_bundle):
    '''
    Test that we can load from the class registry for un-imported classes
    '''
    from owmeta_core.dataobject import DataObject

    class_registry_ctxid = 'http://example.org/class_registry'
    data_ctxid = 'http://example.org/data_context'
    defctxid = 'http://example.org/Person'

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()

    with open(p('tests', 'test_data', 'owmbundletest01_data.n3'), 'rb') as f:
        g.get_context(data_ctxid).parse(f, format='n3')

    with open(p('tests', 'test_data', 'owmbundletest01_class_registry.n3'), 'rb') as f:
        g.get_context(class_registry_ctxid).parse(f, format='n3')

    with open(p('tests', 'test_data', 'owmbundletest01_defctx.n3'), 'rb') as f:
        g.get_context(defctxid).parse(f, format='n3')

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(data_ctxid))
    d.includes.add(make_include_func(defctxid))

    with custom_bundle(d, graph=g, class_registry_ctx=class_registry_ctxid) as testbun, \
            Bundle('test', bundles_directory=testbun.bundles_directory) as bnd:

        bctx = bnd(Context)().stored
        for m in bctx(DataObject)().load():
            assert type(m).__name__ == 'Person'
            break
        else: # no break
            pytest.fail('Expected an object')

    with custom_bundle(d, graph=g) as testbun, \
            Bundle('test', bundles_directory=testbun.bundles_directory) as bnd:

        bctx = bnd(Context)().stored
        for m in bctx(DataObject)().load():
            assert type(m).__name__ != 'Person'
            break
        else: # no break
            pytest.fail('Expected an object')


def test_dependency_class_registry(custom_bundle):
    '''
    Test that we can load from the class registry for un-imported classes
    '''
    from owmeta_core.dataobject import DataObject

    class_registry_ctxid = 'http://example.org/class_registry'
    data_ctxid = 'http://example.org/data_context'
    defctxid = 'http://example.org/Person'

    # Add some triples so the contexts aren't empty -- we can't save an empty context
    g = rdflib.ConjunctiveGraph()

    with open(p('tests', 'test_data', 'owmbundletest01_data.n3'), 'rb') as f:
        g.get_context(data_ctxid).parse(f, format='n3')

    with open(p('tests', 'test_data', 'owmbundletest01_class_registry.n3'), 'rb') as f:
        g.get_context(class_registry_ctxid).parse(f, format='n3')

    with open(p('tests', 'test_data', 'owmbundletest01_defctx.n3'), 'rb') as f:
        g.get_context(defctxid).parse(f, format='n3')

    # Make a descriptor that includes ctx1 and the imports, but not ctx2
    d = Descriptor('test')
    d.includes.add(make_include_func(data_ctxid))
    d.includes.add(make_include_func(defctxid))
    d.dependencies.add(DependencyDescriptor('dep'))

    # Make a dependency that holds the class registry
    dep_d = Descriptor('dep')

    with custom_bundle(dep_d, graph=g, class_registry_ctx=class_registry_ctxid) as depbun, \
            custom_bundle(d, graph=g, bundles_directory=depbun.bundles_directory) as testbun, \
            Bundle('test', bundles_directory=testbun.bundles_directory) as bnd:
        bctx = bnd(Context)().stored
        for m in bctx(DataObject)(ident='http://schema.openworm.org/2020/07/Person#bwithers').load():
            assert type(m).__name__ == 'Person'
            break
        else: # no break
            pytest.fail('Expected an object')


def test_owm_bundle_remote_add_and_list_in_user(shell_helper):
    print(shell_helper.sh('owm bundle remote --user add example-remote http://example.org/remote'))
    output = shell_helper.sh('owm bundle remote --user list')
    assert 'example-remote' in output


def test_owm_bundle_update_nonexistent_remote_error(shell_helper):
    with pytest.raises(CalledProcessError):
        print(shell_helper.sh('owm bundle remote --user update example-remote http://example.org/remote'))


def test_owm_bundle_update_nonexistent_remote_message(shell_helper):
    try:
        print(shell_helper.sh('owm bundle remote --user update example-remote http://example.org/remote',
            stderr=subprocess.STDOUT))
        assert False, "Should have raised CalledProcessError"
    except CalledProcessError as e:
        assertRegexpMatches(e.output.decode('UTF-8'), r'no remote named "example-remote"')


# TODO: Test for bundles with extras that aren't installed
# TODO: Test for bundle remotes that depend on extras that aren't installed
# TODO: Test for bundles with classes that must be retrieved from the class registry
# TODO: Test for installing Python code for mapped classes that cannot be imported
