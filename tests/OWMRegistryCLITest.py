from subprocess import CalledProcessError, PIPE

from pytest import mark, raises

from .TestUtilities import assertRegexpMatches


pytestmark = mark.owm_cli_test


def test_declare_pip_package_name_version_module(owm_project):
    '''
    Declare access with an explicit module, package name, and version
    '''
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_monkey')
    print("MONKEY")
    print(save_out)
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_donkey')
    print("DONKEY")
    print(save_out)
    registry_list_out = owm_project.sh('owm registry module-access declare python-pip'
            ' mypackage 1.4.4 --module-name tests.test_modules.owmclitest05_monkey')
    assertRegexpMatches(registry_list_out,
            'PythonModule(.*tests.test_modules.owmclitest05_monkey.*).*PIPInstall(.*)')


def test_declare_pip_package_not_found(owm_project):
    '''
    Declare access with an explicit package name and version, but find modules installed
    with pip
    '''
    owm_project.make_module('tests')
    owm_project.copy('tests/test_modules', 'tests/test_modules')

    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_monkey')
    print("MONKEY")
    print(save_out)
    save_out = owm_project.sh('owm save tests.test_modules.owmclitest05_donkey')
    print("DONKEY")
    print(save_out)
    with raises(CalledProcessError) as err:
        owm_project.sh(
                'owm registry module-access declare python-pip unknownpackage 1.0.1',
                stderr=PIPE)

    assertRegexpMatches(err.value.stderr.decode('utf-8'), '.*unknownpackage.*')
