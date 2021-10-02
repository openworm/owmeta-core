from unittest.mock import Mock
from os.path import join as pth_join
import os

import transaction
import pytest

from owmeta_core.datasource import DataSource
from owmeta_core.capabilities import OutputFilePathCapability, FilePathCapability
from owmeta_core.capability_providers import TransactionalDataSourceDirProvider as TDSDP


@pytest.fixture
def transaction_manager():
    return transaction.TransactionManager()


def test_abort(tempdir, transaction_manager):
    try:
        with transaction_manager:
            cut = TDSDP(tempdir, transaction_manager)
            ds = DataSource(ident="http://example.org/tdsdp-test")
            cp = cut.provides_to(ds, OutputFilePathCapability())
            outdir = cp.output_file_path()
            with open(pth_join(outdir, 'scratch'), 'w') as f:
                print('flax', file=f)

            raise Mockception('abort')
    except Mockception:
        pass

    assert [] == os.listdir(tempdir)


def test_tpc_abort(tempdir, transaction_manager):
    try:
        with transaction_manager as txn:
            mock_data_manager = Mock(name='failing_datamanager')
            txn.join(mock_data_manager)
            mock_data_manager.tpc_vote.side_effect = Mockception
            mock_data_manager.sortKey.return_value = 'mock_data_manager'
            cut = TDSDP(tempdir, transaction_manager)
            ds = DataSource(ident="http://example.org/tdsdp-test")
            cp = cut.provides_to(ds, OutputFilePathCapability())
            outdir = cp.output_file_path()
            with open(pth_join(outdir, 'scratch'), 'w') as f:
                print('flax', file=f)
    except Mockception:
        pass

    assert [] == os.listdir(tempdir)


def test_input_from_uncommitted_output(tempdir, transaction_manager):
    with transaction_manager:
        cut = TDSDP(tempdir, transaction_manager)
        ds = DataSource(ident="http://example.org/tdsdp-test")
        oprov = cut.provides_to(ds, OutputFilePathCapability())
        ofp = oprov.output_file_path()

        iprov = cut.provides_to(ds, FilePathCapability())
        ifp = iprov.file_path()
        assert ofp == ifp


def test_input_from_committed_output(tempdir, transaction_manager):
    with transaction_manager:
        cut = TDSDP(tempdir, transaction_manager)
        ds = DataSource(ident="http://example.org/tdsdp-test")
        oprov = cut.provides_to(ds, OutputFilePathCapability())
        oprov.output_file_path()

    with transaction_manager:
        iprov = cut.provides_to(ds, FilePathCapability())
        ifp = iprov.file_path()
        assert oprov._committed_path == ifp


def test_no_input_provider_for_no_file(tempdir, transaction_manager):
    try:
        with transaction_manager:
            cut = TDSDP(tempdir, transaction_manager)
            ds = DataSource(ident="http://example.org/tdsdp-test")
            oprov = cut.provides_to(ds, OutputFilePathCapability())
            oprov.output_file_path()
            raise Mockception('abort')
    except Mockception:
        pass

    with transaction_manager:
        iprov = cut.provides_to(ds, FilePathCapability())
        assert iprov is None

# Other tests to try:
# - any invalid transition should fail (probably want to explicitly maintain a transaction
#   state and allowed transitions and just fail on invalid ones)
# - lock file deletion
#
# Other features to add
# - Grabbing the default transaction manager if none is provided


class Mockception(Exception):
    pass
