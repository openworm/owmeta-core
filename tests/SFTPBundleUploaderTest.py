from os.path import join as p
from textwrap import dedent
from unittest.mock import Mock

import pytest

try:
    from paramiko import DSSKey, RSAKey, ECDSAKey, Ed25519Key, SSHException
    from paramiko.hostkeys import HostKeyEntry
    from cryptography.hazmat.primitives.asymmetric import ec
except ImportError:
    pytest.skip('Skipping SFTP bundle upload tests due to lack of "paramiko" or "cryptography"',
            allow_module_level=True)

from owmeta_core.bundle import URLConfig
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


def test_sftp_remote_no_url():
    self = Mock()
    self._url_config = None
    with pytest.raises(GenericUserError):
        sftp_remote(self)


def test_sftp_remote_non_sftp_url():
    self = Mock()
    self._url_config = URLConfig('http://example.org/hi-mom')
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


def test_sftp_remote_add_rsa_host_key(genhostkey):
    self, hostkeyfile = genhostkey(RSAKey)
    sftp_remote(self, host_key=hostkeyfile, host_key_type='RSA')

    assert isinstance(self._url_config.host_key, RSAKey)


def test_sftp_remote_add_ecdsa_host_key_without_type_specified(genhostkey):
    self, hostkeyfile = genhostkey(ECDSAKey)
    sftp_remote(self, host_key=hostkeyfile)

    assert isinstance(self._url_config.host_key, ECDSAKey)


def test_sftp_remote_add_ecdsa_nistp384_host_key_with_type_specified(genhostkey):
    self, hostkeyfile = genhostkey(ECDSAKey, curve=ec.SECP384R1())
    sftp_remote(self, host_key=hostkeyfile, host_key_type='ECDSA')

    # Don't really care what curve, but the different key types have different key file
    # formats. Default is 256P
    assert isinstance(self._url_config.host_key, ECDSAKey)


def test_sftp_remote_add_ecdsa_nistp521_host_key_with_type_specified(genhostkey):
    self, hostkeyfile = genhostkey(ECDSAKey, curve=ec.SECP521R1())
    sftp_remote(self, host_key=hostkeyfile, host_key_type='ECDSA')

    # Don't really care what curve, but the different key types have different key file
    # formats. Default is 256P
    assert isinstance(self._url_config.host_key, ECDSAKey)


def test_sftp_remote_add_host_key_type_not_found(genhostkey):
    self, hostkeyfile = genhostkey(RSAKey)
    with pytest.raises(GenericUserError, match=r'DSS'):
        sftp_remote(self, host_key=hostkeyfile, host_key_type='DSS')


def test_sftp_remote_add_hostkey_host_not_found(tempdir):
    self = Mock()
    self._url_config = SFTPURLConfig('sftp://example.org')
    hostkeyfile = p(tempdir, 'known_hosts')
    open(hostkeyfile, 'w').close()
    with pytest.raises(GenericUserError):
        sftp_remote(self, host_key=hostkeyfile)


def test_sftp_remote_add_hostkey_file_not_found(tempdir):
    self = Mock()
    self._url_config = SFTPURLConfig('sftp://example.org')
    hostkeyfile = p(tempdir, 'known_hosts')
    with pytest.raises(GenericUserError):
        sftp_remote(self, host_key=hostkeyfile)


def test_sftp_remote_add_host_key_unrecognized_type(genhostkey):
    self, hostkeyfile = genhostkey(RSAKey)
    with pytest.raises(GenericUserError, match=r'UNCE-UNCE-UNCE'):
        sftp_remote(self, host_key=hostkeyfile, host_key_type='UNCE-UNCE-UNCE')


def test_sftp_remote_updates_remote(genhostkey):
    self, hostkeyfile = genhostkey(RSAKey)
    sftp_remote(self, host_key=hostkeyfile)
    self._write_remote.assert_called()


@pytest.fixture
def genkey(tempdir):
    def fun(keytype, *args, **kwargs):
        self, keyfile, key = genkey_func(tempdir, keytype, *args, **kwargs)
        return self, keyfile
    return fun


@pytest.fixture
def genhostkey(tempdir):
    def fun(keytype, *args, **kwargs):
        self, keyfile, key = genkey_func(tempdir, keytype, *args, **kwargs)
        hostkeyfile = p(tempdir, 'known_hosts')
        with open(hostkeyfile, 'w') as f:
            print(HostKeyEntry(['example.org'], key).to_line(), file=f)
        return self, hostkeyfile
    return fun


def genkey_func(tempdir, keytype, *args, **kwargs):
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
        with open(keyfile, 'w+') as f:
            f.write(dedent(keydata))
            f.seek(0)
            key = Ed25519Key(file_obj=f)
    else:
        if keytype is RSAKey and not args and 'bits' not in kwargs:
            args = list(args)
            args.append(512)
        with open(keyfile, 'w') as f:
            key = keytype.generate(*args, **kwargs)
            key.write_private_key(f)
    return self, keyfile, key
