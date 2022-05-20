from pytest import mark

from owmeta_core.git_repo import GitRepoProvider


pytestmark = mark.inttest


def test_init_is_not_dirty(tmp_path):
    cut = GitRepoProvider()
    cut.init(tmp_path)
    assert not cut.is_dirty()


def test_add_file_dirty(tmp_path):
    cut = GitRepoProvider()
    cut.init(tmp_path)
    newfile = tmp_path / "file"
    open(newfile, "w").close()
    cut.add([str(newfile)])
    assert cut.is_dirty()


def test_add_file_other_commited_path_not_dirty(tmp_path):
    cut = GitRepoProvider()
    cut.init(tmp_path)
    newfile1 = tmp_path / "file1"
    newfile2 = tmp_path / "file2"
    open(newfile1, "w").close()
    cut.add([str(newfile1)])
    cut.commit('First file')

    open(newfile2, "w").close()
    assert not cut.is_dirty(newfile1)


def test_add_file_dirty_path(tmp_path):
    cut = GitRepoProvider()
    cut.init(tmp_path)
    newfile1 = tmp_path / "file1"
    newfile2 = tmp_path / "file2"
    open(newfile1, "w").close()
    cut.add([str(newfile1)])
    cut.commit('First file')

    open(newfile2, "w").close()
    cut.add([str(newfile2)])
    assert cut.is_dirty(newfile2)
