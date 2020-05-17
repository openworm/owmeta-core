'''
Logic for a dumb SFTP bundle uploader
'''

from os.path import basename
from urllib.parse import urlparse

from paramiko import Transport, SFTPClient, RSAKey, ECDSAKey, DSSKey, Ed25519Key, HostKeys

from ..command_util import GenericUserError
from ..bundle import Uploader, URLConfig, ensure_archive

KEYTYPES = {'RSA': RSAKey,
            'ECDSA': ECDSAKey,
            'DSA': DSSKey,
            'DSS': DSSKey,
            'ED25519': Ed25519Key}


KNOWN_HOSTS_KEY_TYPE_MAP = {'RSA': ('ssh-rsa',),
                            'ECDSA': tuple(ECDSAKey.supported_key_format_identifiers()),
                            'DSA': ('ssh-dss',),
                            'DSS': ('ssh-dss',),
                            'ED25519': ('ssh-ed25519',)}


class DumbSFTPUploader(Uploader):
    '''
    A dumb SFTP uploader that just sends a bundle to a remote directory.

    The server has to decide what to do with the bundle (e.g., putting it where it can be
    downloaded)
    '''
    def __init__(self, upload_url):
        '''
        Parameters
        ----------
        upload_url : str or .URLConfig
            URL string or accessor config
        '''
        if (isinstance(upload_url, URLConfig) and
                not isinstance(upload_url, SFTPURLConfig)):
            upload_url = upload_url.url

        if isinstance(upload_url, str):
            sftp_url = SFTPURLConfig(upload_url)
        elif isinstance(upload_url, SFTPURLConfig):
            sftp_url = upload_url

        self.sftp_url = sftp_url

    def upload(self, bundle_path):
        '''
        Upload the given bundle to the SFTP host specified by the `SFTPURLConfig` specified
        at initialization.

        If the URL config does not specify a host key, then host-key checking will not be
        done.

        If the URL config does not specify auth details (password or identity), then
        '''
        with ensure_archive(bundle_path) as archive_path:
            host = self.sftp_url.hostname
            port = self.sftp_url.port
            path = self.sftp_url.path
            sockargs = (host,) + ((port,) if port else ())
            with Transport(sockargs) as transport:
                transport.connect(**self._build_connect_args())
                with SFTPClient.from_transport(transport) as sftp:
                    bn = basename(archive_path)
                    sftp.put(archive_path, '%s/%s' % (path, bn))

    def _build_connect_args(self):
        connect_args = dict()
        for a in [('username',),
                  ('password',),
                  ('identity', 'pkey'),
                  ('host_key', 'hostkey')]:
            self._set_if_configured(connect_args, *a)
        return connect_args

    def _set_if_configured(self, connect_args, sftp_key, connect_key=None):
        connect_key = connect_key or sftp_key
        sftp_url = self.sftp_url
        if getattr(sftp_url, sftp_key):
            connect_args[connect_key] = getattr(sftp_url, sftp_key)

    def can_upload(self, bundle_path):
        return True

    @classmethod
    def can_upload_to(self, accessor_config):
        return (isinstance(accessor_config, URLConfig) and
                accessor_config.url.startswith('sftp://'))


DumbSFTPUploader.register()


class SFTPURLConfig(URLConfig):
    '''
    The URL path is interpreted as a relative path unless a double-slash is used. For
    instance::

        sftp://example.org//target/dir

    would be presented to the SFTP server as ``/target/dir`` for all SFTP operations.
    '''

    def __init__(self, *args, password=None, identity=None, host_key=None, **kwargs):
        '''
        Parameters
        ----------
        password : str, optional
            Password for connecting to the SFTP server
        identity : paramiko.pkey.PKey, optional
            Private key for authenticating to the SFTP server
        host_key : paramiko.pkey.PKey, optional
            Public key of the SFTP server for host-key checking
        '''
        super(SFTPURLConfig, self).__init__(*args, **kwargs)
        parsed = urlparse(self.url)
        if parsed.scheme != 'sftp':
            raise ValueError(f'Given URL "{self.url}" is not an SFTP URL')
        self.path = parsed.path[1:] if parsed.path.startswith('/') else parsed.path
        self.hostname = parsed.hostname
        self.port = parsed.port
        self.username = parsed.username
        self.password = password or parsed.password
        self.identity = identity
        self.host_key = host_key


SFTPURLConfig.register('sftp')


def sftp_remote(self, *, password=None,
        identity_type='RSA', identity=None,
        host_key_type=None, host_key=None):
    '''
    Parameters for SFTP remote accessors

    Parameters
    ----------
    password : str
        Password for the SFTP server.
        CAUTION: Will be stored in plain-text in the project directory, though it will not
        be shared. optional
    identity_type : str
        The key type for the identity file. "RSA", "ECDSA", "DSS", "DSA", "ED25519".
        optional, default is "RSA".
    identity : str
        Path to the identity file (e.g., '~/.ssh/id_rsa')
    host_key_type : str
        The key type for the host key file. "RSA", "ECDSA", "DSS", "DSA", "ED25519".
        optional. The default is to accept any host key type for this host.
    host_key : str
        Host key file (e.g., '~/.ssh/known_hosts')
    '''
    if self._url_config is None:
        raise GenericUserError('An SFTP URL must be specified for SFTP accessors')

    if not isinstance(self._url_config, SFTPURLConfig):
        raise GenericUserError(f'The specified URL, {self._url_config} is not an SFTP URL')

    self._url_config.password = password
    if identity:
        try:
            keytype = KEYTYPES[identity_type.upper()]
        except KeyError:
            raise GenericUserError('The only key types accepted are {}, but we were given'
                    ' {}'.format(', '.join(KEYTYPES), identity_type))

        if keytype is Ed25519Key:
            self._url_config.identity = keytype(filename=identity)
        else:
            self._url_config.identity = keytype.from_private_key_file(identity)
    if host_key:
        try:
            known_hosts = HostKeys(host_key)
        except FileNotFoundError:
            raise GenericUserError(f'The given host key file {host_key} could not be found')
        hostkeys = known_hosts.lookup(self._url_config.hostname)
        if not hostkeys:
            raise GenericUserError('There are no host keys associated with'
                    f' {self._url_config.hostname}')
        if host_key_type:
            try:
                keys = KNOWN_HOSTS_KEY_TYPE_MAP[host_key_type]
                for k in keys:
                    pkey = hostkeys.get(k)
                    if pkey:
                        break
                else: # no break
                    raise GenericUserError(f'There are no host keys with the given type {host_key_type}')
            except KeyError:
                raise GenericUserError(('Unrecognized key type {}.'
                        ' Recognized key types: {}').format(host_key_type,
                            ', '.join(KNOWN_HOSTS_KEY_TYPE_MAP)))
        else:
            for pkey in hostkeys.values():
                break
        self._url_config.host_key = pkey
    self._write_remote()
