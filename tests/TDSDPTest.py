from unittest.mock import Mock
from os.path import join as pth_join
import os

import transaction

from owmeta_core.datasource import DataSource
from owmeta_core.capability_providers import TransactionalDataSourceDirProvider as TDSDP


def test_abort(tempdir):
    try:
        with transaction.manager:
            cut = TDSDP(tempdir, transaction.manager)
            ds = DataSource(ident="http://example.org/tdsdp-test")
            cp = cut.provides_to(ds)
            outdir = cp.file_path()
            with open(pth_join(outdir, 'scratch'), 'w') as f:
                print('flax', file=f)

            raise Mockception('abort')
    except Mockception:
        pass

    assert [] == os.listdir(tempdir)


def test_tpc_abort(tempdir):
    try:
        with transaction.manager as txn:
            mock_data_manager = Mock(name='failing_datamanager')
            txn.join(mock_data_manager)
            mock_data_manager.tpc_vote.side_effect = Mockception
            mock_data_manager.sortKey.return_value = 'mock_data_manager'
            cut = TDSDP(tempdir, transaction.manager)
            ds = DataSource(ident="http://example.org/tdsdp-test")
            cp = cut.provides_to(ds)
            outdir = cp.file_path()
            with open(pth_join(outdir, 'scratch'), 'w') as f:
                print('flax', file=f)
    except Mockception:
        pass

    assert [] == os.listdir(tempdir)

# Other tests to try:
# - any invalid transition should fail (probably want to explicitly maintain a transaction
#   state and allowed transitions and just fail on invalid ones)
# - lock file deletion
#
# Other features to add
# - Grabbing the default transaction manager if none is provided


class Mockception(Exception):
    pass
