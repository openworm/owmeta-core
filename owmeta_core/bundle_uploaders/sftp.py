from os.path import basename
from urllib.parse import urlparse

from paramiko import Transport
from paramiko.sftp_client import SFTPClient

from ..bundle import Uploader, URLConfig


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
        upload_url : str or URLConfig
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
        with self.ensure_archive(bundle_path) as archive_path:
            host = self.sftp_url.hostname
            port = self.sftp_url.port
            path = self.sftp_url.path
            sockargs = host + ((port,) if port else ())
            with Transport(sockargs) as transport:
                transport.connect(**self._build_connect_args())
                with SFTPClient.from_transport(transport) as sftp:
                    bn = basename(archive_path)
                    sftp.put(archive_path, '%s/%s' % (path, bn))

    def _build_connect_args(self):
        connect_args = dict()
        for a in [('username',),
                  ('password',),
                  ('public_key', 'pkey'),
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
                accessor_config.url.startswith('stfp://'))


class SFTPURLConfig(URLConfig):

    def __init__(self, *args, password=None, public_key=None, host_key=None, **kwargs):
        '''
        Parameters
        ----------
        password : str, optional
            Password for connecting to the SFTP server
        public_key : paramiko.pkey.PKey, optional
            Public key for authenticating to the SFTP server
        host_key : paramiko.pkey.PKey, optional
            Public key of the SFTP server for host_key checking
        '''
        super(SFTPURLConfig, self).__init__(*args, **kwargs)
        parsed = urlparse(self.url)
        self.path = parsed.path
        self.hostname = parsed.hostname
        self.port = parsed.port
        self.username = parsed.username
        self.password = password or parsed.password
        self.public_key = public_key
        self.host_key = host_key
