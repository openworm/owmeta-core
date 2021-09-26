from os.path import join as p, exists, islink
from os import mkdir, stat

import pytest

from owmeta_core.capabilities import (FilePathProvider,
                                      FilePathCapability,
                                      OutputFilePathProvider,
                                      OutputFilePathCapability)
from owmeta_core.data_trans.local_file_ds import LocalFileDataSource, CommitOp


def test_accept_provider():
    class Provider(FilePathProvider):
        def file_path(self):
            return 'tests'

    lfds = LocalFileDataSource()
    lfds.accept_capability_provider(FilePathCapability(), Provider())
    assert lfds.basedir() == 'tests'


SOURCE_FILE_CONTENT = 'Hello, Dolly!'


@pytest.fixture
def lfds_with_file(tmp_path):
    outdir = p(tmp_path, 'output')
    mkdir(outdir)

    class OutputProvider(OutputFilePathProvider):
        def file_path(self):
            return outdir

    lfds = LocalFileDataSource(file_name='dolly.txt')
    source_file = p(tmp_path, 'source')
    with open(source_file, 'w') as f:
        f.write(SOURCE_FILE_CONTENT)
    lfds.source_file_path = source_file
    lfds.accept_capability_provider(OutputFilePathCapability(), OutputProvider())
    return lfds


@pytest.mark.inttest
def test_commit_default(lfds_with_file):
    cut = lfds_with_file

    cut.after_transform()

    with open(cut.full_output_path()) as f:
        assert SOURCE_FILE_CONTENT == f.read()


@pytest.mark.inttest
def test_commit_rename(lfds_with_file):
    cut = lfds_with_file
    cut.commit_op = CommitOp.RENAME

    cut.after_transform()

    with open(cut.full_output_path()) as f:
        assert SOURCE_FILE_CONTENT == f.read()

    assert not exists(cut.source_file_path)


@pytest.mark.inttest
def test_commit_symlink(lfds_with_file):
    cut = lfds_with_file
    cut.commit_op = CommitOp.SYMLINK

    cut.after_transform()

    with open(cut.full_output_path()) as f:
        assert SOURCE_FILE_CONTENT == f.read()
    assert islink(cut.full_output_path())
    assert exists(cut.source_file_path)


@pytest.mark.inttest
def test_commit_link(lfds_with_file):
    cut = lfds_with_file
    cut.commit_op = CommitOp.HARDLINK

    cut.after_transform()

    with open(cut.full_output_path()) as f:
        assert SOURCE_FILE_CONTENT == f.read()
    statbuf = stat(cut.full_output_path())
    assert exists(cut.source_file_path)
    assert statbuf.st_nlink == 2


def test_commit_no_commit_op(lfds_with_file):
    cut = lfds_with_file
    cut.commit_op = None

    with pytest.raises(TypeError):
        cut.after_transform()
