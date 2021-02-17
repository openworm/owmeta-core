from contextlib import contextmanager
import logging
from os import scandir, walk
from os.path import join as p, relpath, realpath, abspath, isdir, dirname
import json
import shutil
import tarfile
import tempfile

from .common import (fmt_bundle_directory, validate_manifest, find_bundle_directory,
                     bundle_tree_filter)
from .exceptions import NotABundlePath


L = logging.getLogger(__name__)


class Unarchiver(object):
    '''
    Unpacks an archive file (e.g., a `tar.xz`) of a bundle
    '''
    def __init__(self, bundles_directory=None):
        '''
        Parameters
        ----------
        bundles_directory : str, optional
            The directory under which bundles should be unpacked. Typically the bundle
            cache directory.
        '''
        self.bundles_directory = bundles_directory

    def unpack(self, input_file, target_directory=None):
        '''
        Unpack the archive file

        If `target_directory` is provided, and `bundles_directory` is provided at
        initialization, then if the bundle manifest doesn't match the expected archive
        path, then an exception is raised.

        Parameters
        ----------
        input_file : str or :term:`file object`
            The archive file
        target_directory : str, optional
            The path where the archive should be unpacked. If this argument is not
            provided, then the target directory is derived from `bundles_directory` (see
            `fmt_bundle_directory`)

        Raises
        ------
        NotABundlePath
            Thrown in one of these conditions:

            - If the `input_file` is not in an expected format (lzma-zipped TAR file)
            - If the `input_file` does not have a "manifest" file
            - If the `input_file` manifest file is invalid or is not a regular file (see
              `validate_manifest` for further details)
            - If the `input_file` is a file path and the corresponding file is not found
        TargetDirectoryMismatch
            Thrown when both a `bundles_directory` has been set at initialization and a
            `target_directory` is passed to this method and the path under
            `bundles_directory` indicated by the manifest in the `input_file` does not
            agree with `target_directory`
        '''

        # - If we were given a target directory, just unpack there...no complications
        #
        # - If we weren't given a target directory, then we have to extract the manifest,
        # read the version and name, then create the target directory
        if not self.bundles_directory and not target_directory:
            raise UnarchiveFailed('Neither a bundles_directory nor a target_directory was'
                    ' provided. Cannot determine where to extract %s archive to.' %
                    input_file)
        try:
            self._unpack(input_file, target_directory)
        except tarfile.ReadError:
            raise NotABundlePath(self._bundle_file_name(input_file),
                'Unable to read archive file')

    @classmethod
    def _bundle_file_name(cls, input_file):
        '''
        Try to extract the bundle file name from `input_file`
        '''
        if isinstance(input_file, str):
            file_name = input_file
        elif hasattr(input_file, 'name'):
            file_name = input_file.name
        else:
            file_name = 'bundle archive file'

        return file_name

    def _unpack(self, input_file, target_directory):
        with self.to_tarfile(input_file) as ba:
            expected_target_directory = self._target_path_from_archive_manifest(ba, input_file)

            if (target_directory and expected_target_directory and
                    expected_target_directory != target_directory):
                raise TargetDirectoryMismatch(target_directory, expected_target_directory)
            elif not target_directory:
                target_directory = expected_target_directory

            L.debug('extracting %s to %s', input_file, target_directory)
            target_directory_empty = True
            try:
                for _ in scandir(target_directory):
                    target_directory_empty = False
                    break
            except FileNotFoundError:
                pass
            if not target_directory_empty:
                raise UnarchiveFailed('Target directory, "%s", is not empty' %
                        target_directory)
            try:
                ArchiveExtractor(target_directory, ba).extract()
            except _BadArchiveFilePath:
                shutil.rmtree(target_directory)
                file_name = self._bundle_file_name(input_file)
                raise NotABundlePath(file_name, 'Archive contains files that point'
                    ' outside of the target directory')

    def _target_path_from_archive_manifest(self, ba, input_file):
        with self._manifest(ba, input_file) as manifest_data:
            bundle_id = manifest_data['id']
            bundle_version = manifest_data['version']
            if self.bundles_directory:
                return fmt_bundle_directory(self.bundles_directory, bundle_id, bundle_version)

    @classmethod
    def manifest(cls, bundle_tarfile, input_file=None):
        '''
        Get the manifest file from a bundle archive

        Parameters
        ----------
        bundle_tarfile : tarfile.TarFile
            Tarfile, ostensibly containing bundle data
        input_file : :term:`file object` or str, optional
            Name of the tar file. Will attempt to extract it from the tarfile if not given
        '''
        return cls._manifest(bundle_tarfile, input_file)

    @classmethod
    @contextmanager
    def _manifest(cls, ba, input_file):
        try:
            ef = ba.extractfile('./manifest')
        except KeyError:
            try:
                # Both ./manifest and manifest are valid...just have to try both of them
                ef = ba.extractfile('manifest')
            except KeyError:
                file_name = cls._bundle_file_name(input_file)
                raise NotABundlePath(file_name, 'archive has no manifest')

        with ef as manifest:
            file_name = cls._bundle_file_name(input_file)
            if manifest is None:
                raise NotABundlePath(file_name, 'archive manifest is not a regular file')
            manifest_data = json.load(manifest)
            validate_manifest(file_name, manifest_data)
            yield manifest_data

    def __call__(self, *args, **kwargs):
        '''
        Unpack the archive file
        '''
        return self.unpack(*args, **kwargs)

    @classmethod
    @contextmanager
    def to_tarfile(cls, input_file):
        if isinstance(input_file, str):
            try:
                archive_file = open(input_file, 'rb')
            except FileNotFoundError:
                file_name = cls._bundle_file_name(input_file)
                raise NotABundlePath(file_name, 'file not found')

            with archive_file as f, cls._to_tarfile0(f) as ba:
                yield ba
        else:
            if hasattr(input_file, 'read'):
                with cls._to_tarfile0(input_file) as ba:
                    yield ba

    @classmethod
    @contextmanager
    def _to_tarfile0(cls, f):
        with tarfile.open(mode='r:xz', fileobj=f) as ba:
            yield ba


class ArchiveExtractor(object):
    '''
    Extracts `tarfile` archives
    '''
    def __init__(self, targetdir, tarfile):
        '''
        Parameters
        ----------
        targetdir : str
            The directory to which the archive will be extracted
        tarfile : tarfile.TarFile
            The file to extract
        '''
        self._targetdir = targetdir
        self._tarfile = tarfile

    def extract(self):
        '''
        Extract the tarfile to the target directory
        '''
        self._tarfile.extractall(self._targetdir, members=self._safemembers())

    def _realpath(self, path):
        return realpath(abspath(path))

    def _badpath(self, path, base=None):
        # joinpath will ignore base if path is absolute
        if base is None:
            base = self._targetdir
        return not self._realpath(p(self._targetdir, path)).startswith(base)

    def _badlink(self, info):
        # Links are interpreted relative to the directory containing the link
        # TODO: Test this
        tip = self._realpath(p(self._targetdir, dirname(info.name)))
        return self._badpath(info.linkname, base=tip)

    def _safemembers(self):
        for finfo in self._tarfile.members:
            if self._badpath(finfo.name):
                raise _BadArchiveFilePath(finfo.name, 'Path is outside of base path "%s"' % self._targetdir)
            elif finfo.issym() and self._badlink(finfo):
                raise _BadArchiveFilePath(finfo.name,
                        'Hard link points to "%s", outside of base path "%s"' % (finfo.linkname, self._targetdir))
            elif finfo.islnk() and self._badlink(finfo):
                raise _BadArchiveFilePath(finfo.name,
                        'Symlink points to "%s", outside of "%s"' % (finfo.linkname,
                            self._targetdir))
            else:
                yield finfo

    def validate(self):
        for _ in self._tarfile._safemembers():
            pass


class _BadArchiveFilePath(Exception):
    '''
    Thrown when an archive file path points outside of a given base directory
    '''
    def __init__(self, archive_file_path, error):
        '''
        Parameters
        ----------
        archive_file_path : str
            The path to the archive file
        error : str
            Explanation of why the archive path is bad
        '''
        super(_BadArchiveFilePath, self).__init__(
                'Disallowed archive file %s: %s' %
                (archive_file_path, error))
        self.archive_file_path = archive_file_path
        self.error = error


class Archiver(object):
    '''
    Archives a bundle directory tree
    '''
    def __init__(self, target_directory, bundles_directory=None):
        '''
        Parameters
        ----------
        target_directory : str
            Where to place archives.
        bundles_directory : str, optional
            Where the bundles are. If not provided, then this archiver can only pack
            bundles when given a specific bundle's directory
        '''
        self.target_directory = target_directory
        self.bundles_directory = bundles_directory

    def pack(self, bundle_id=None, version=None, *, bundle_directory=None, target_file_name=None):
        '''
        Pack an installed bundle into an archive file

        Parameters
        ----------
        bundle_id : str, optional
            ID of the bundle to pack. If omitted, the `bundle_directory` must be provided
        version : int, optional
            Bundle version
        bundle_directory : str, optional
            Bundle directory. If omitted, `bundle_id` must be provided. If provided,
            `bundle_id` and `version` are ignored
        target_file_name : str, optional
            Name of the archive file. If not provided, the name will be 'bundle.tar.xz'
            and will placed in the `target_directory`. Relative paths are relative to
            `target_directory`

        Raises
        ------
        BundleNotFound
            Thrown when the bundle with the given ID cannot be found, or cannot be found
            at the demanded version
        ArchiveTargetPathDoesNotExist
            Thrown when the path to the desired target file does not exist
        '''
        if not (bundle_id or bundle_directory):
            raise ValueError('Either bundle_id or bundle_directory arguments must be provided')

        if not target_file_name:
            target_file_name = 'bundle.tar.xz'

        target_path = p(self.target_directory, target_file_name)

        if bundle_directory:
            bnd_directory = bundle_directory
        else:
            if self.bundles_directory is None:
                raise Exception('Cannot generate a bundle directory -- bundles_directory'
                        ' has not been provided')
            bnd_directory = find_bundle_directory(self.bundles_directory, bundle_id, version)

        accept = self._filter
        try:
            _tf = tarfile.open(target_path, mode='w:xz')
        except FileNotFoundError as e:
            if e.filename == target_path:
                raise ArchiveTargetPathDoesNotExist(target_path) from e
            raise
        else:
            with _tf as tf:
                for dirpath, dirs, files in walk(bnd_directory):
                    for f in files:
                        fpath = p(dirpath, f)
                        rpath = relpath(fpath, start=bnd_directory)
                        if accept(rpath, fpath):
                            tf.add(fpath, rpath)
        return target_path

    def _filter(self, path, fullpath):
        '''
        Filters out file names that are not to be included in a bundle
        '''
        return bundle_tree_filter(path, fullpath)


@contextmanager
def ensure_archive(bundle_path):
    '''
    Produce an archive path from a bundle path whether the given path is an archive or not

    Parameters
    ----------
    bundle_path : str
        The path to a bundle directory or archive
    '''
    archive_path = bundle_path
    with tempfile.TemporaryDirectory() as tempdir:
        if isdir(bundle_path):
            archive_path = Archiver(tempdir).pack(
                    bundle_directory=bundle_path, target_file_name='bundle.tar.xz')
        if not tarfile.is_tarfile(archive_path):
            # We don't really care about the TAR file being properly formatted here --
            # it's up to the server to tell us it can't process the bundle. We just
            # check if it's a TAR file for the convenience of the user.
            raise NotABundlePath(bundle_path, 'Expected a directory or a tar file')
        yield archive_path


class ArchiveTargetPathDoesNotExist(Exception):
    '''
    Thrown when the `Archiver` target path does not exist
    '''


class UnarchiveFailed(Exception):
    '''
    Thrown when an `Unarchiver` fails for some reason not covered by other
    '''


class TargetDirectoryMismatch(UnarchiveFailed):
    '''
    Thrown when the target path doesn't agree with the bundle manifest
    '''
    def __init__(self, target_directory, expected_target_directory):
        super(TargetDirectoryMismatch, self).__init__(
                'Target directory "%s" does not match expected directory "%s" for the'
                ' bundle manifest.'
                % (target_directory, expected_target_directory))
        self.target_directory = target_directory
        self.expected_target_directory = expected_target_directory
