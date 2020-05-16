from os.path import join as p
from textwrap import dedent
from unittest.mock import Mock

import pytest

try:
    from paramiko import DSSKey, RSAKey, ECDSAKey, Ed25519Key, SSHException
except ImportError:
    pytest.skip("Skipping SFTP bundle upload tests due to lack of paramiko", allow_module_level=True)

from owmeta_core.command_util import GenericUserError
from owmeta_core.bundle_uploaders.sftp import DumbSFTPUploader, SFTPURLConfig, sftp_remote


def test_uploader_preserves_url_configs():
    cfg = SFTPURLConfig('sftp://example.org/sftp', password='yo-dawg')
    cut = DumbSFTPUploader(cfg)
    assert 'yo-dawg' == cut.sftp_url.password


def test_uploader_str_url_config():
    cut = DumbSFTPUploader('sftp://example.org/sftp')
    assert 'sftp' == cut.sftp_url.path


def test_SFTPURLConfig_fails_on_non_sftp_url():
    with pytest.raises(ValueError):
        SFTPURLConfig('http://example.org/sftp', password='yo-dawg')


def test_SFTPURLConfig_relative_path():
    cfg = SFTPURLConfig('sftp://example.org/sftp', password='yo-dawg')
    assert 'sftp' == cfg.path


def test_SFTPURLConfig_absolute_path():
    cfg = SFTPURLConfig('sftp://example.org//sftp', password='yo-dawg')
    assert '/sftp' == cfg.path


def test_SFTPURLConfig_password_arg_url_password():
    cfg = SFTPURLConfig('sftp://:whatup@example.org//sftp', password='yo-dawg')
    assert cfg.password == 'yo-dawg'


def test_SFTPURLConfig_url_password():
    cfg = SFTPURLConfig('sftp://:whatup@example.org//sftp')
    assert cfg.password == 'whatup'


def test_SFTPURLConfig_url_user():
    cfg = SFTPURLConfig('sftp://blahblah@example.org//sftp')
    assert cfg.username == 'blahblah'


def test_sftp_remote_not_an_sftp_url():
    self = Mock()
    self._url_config = None
    with pytest.raises(GenericUserError):
        sftp_remote(self)


def test_sftp_remote_add_password():
    self = Mock()
    self._url_config = SFTPURLConfig('sftp://example.org')
    sftp_remote(self, password='passyword')

    assert self._url_config.password == 'passyword'


def test_sftp_remote_add_rsa_identity(genkey):
    self, keyfile = genkey(RSAKey, 512)
    sftp_remote(self, identity=keyfile)

    assert isinstance(self._url_config.identity, RSAKey)


def test_sftp_remote_add_ecdsa_identity(genkey):
    self, keyfile = genkey(ECDSAKey)
    sftp_remote(self, identity=keyfile, identity_type='ECDSA')

    assert isinstance(self._url_config.identity, ECDSAKey)


def test_sftp_remote_add_wrong_identity_type(genkey):
    self, keyfile = genkey(ECDSAKey)
    with pytest.raises(SSHException):
        sftp_remote(self, identity=keyfile, identity_type='RSA')


def test_sftp_remote_add_unknown_identity_type(genkey):
    self, keyfile = genkey(ECDSAKey)
    with pytest.raises(GenericUserError):
        sftp_remote(self, identity=keyfile, identity_type='URBLDURBLE0')


def test_sftp_remote_add_dsa_identity(genkey):
    self, keyfile = genkey(DSSKey)
    sftp_remote(self, identity=keyfile, identity_type='DSS')

    assert isinstance(self._url_config.identity, DSSKey)


def test_sftp_remote_add_ed25519_identity(genkey):
    self, keyfile = genkey(Ed25519Key)
    sftp_remote(self, identity=keyfile, identity_type='ED25519')

    assert isinstance(self._url_config.identity, Ed25519Key)


@pytest.fixture
def genkey(tempdir):
    def fun(keytype, *args, **kwargs):
        self = Mock()
        self._url_config = SFTPURLConfig('sftp://example.org')
        keyfile = p(tempdir, 'blah.key')
        if keytype is Ed25519Key:
            # Ed25519Key doesn't have a 'generate' method
            keydata = '''\
            -----BEGIN OPENSSH PRIVATE KEY-----
            b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
            QyNTUxOQAAACBUzXzTlCOstpcekcQyBc7sUydNML0Mai/qbgAviUd2XwAAAJj2toqH9raK
            hwAAAAtzc2gtZWQyNTUxOQAAACBUzXzTlCOstpcekcQyBc7sUydNML0Mai/qbgAviUd2Xw
            AAAEDFOCEqWHTW/7hDx05+gkBFyDuA1Ljk/5xVf/pfeOIb7lTNfNOUI6y2lx6RxDIFzuxT
            J00wvQxqL+puAC+JR3ZfAAAAFG1hcmt3QGFyY3RpYy1vdXRwb3N0AQ==
            -----END OPENSSH PRIVATE KEY-----'''
            with open(keyfile, 'w') as f:
                f.write(dedent(keydata))
        else:
            with open(keyfile, 'w') as f:
                keytype.generate(*args, **kwargs).write_private_key(f)
        return self, keyfile
    return fun
