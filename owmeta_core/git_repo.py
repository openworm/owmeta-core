from os.path import exists
import logging

from git import Repo


L = logging.getLogger(__name__)


class GitRepoProvider:
    '''
    Provides a project repository for `~.command.OWM` backed by a Git repository
    '''
    def __init__(self):
        self._repo = None
        self.base = None

    def init(self, base=None):
        base = self.base if not base else base
        self._repo = Repo.init(base)

    def add(self, files):
        self.repo().index.add(files)

    def remove(self, files, recursive=False):
        self.repo().index.remove(files, r=recursive)

    def reset(self, *paths):
        from git.refs.head import HEAD
        repo = self.repo()
        HEAD(repo).reset(paths=paths)
        paths = [p for p in paths if exists(p)]
        repo.index.checkout(paths=paths, force=True)

    def commit(self, msg):
        self.repo().index.commit(msg)

    def repo(self):
        if self._repo is None:
            self._repo = Repo(self.base)
        return self._repo

    def clone(self, url, base, progress=None, **kwargs):
        '''
        Parameters
        ----------
        url : str
            URL to clone from
        base : str
            Directory to clone into
        progress : `tqdm.tqdm`-like
            Must support a `progress.update` method accepting the amount to add to total
            progress (see https://tqdm.github.io/docs/tqdm/#update)
        '''
        # Techincally, url and base can be "path-like", but we don't make it part of the
        # formal interface by documenting that
        if progress is not None:
            try:
                progress = _CloneProgress(progress)
            except TypeError:
                L.warning("Progress reporter does not have the necessary interface for "
                " reporting clone progress", exc_info=True)
        Repo.clone_from(url, base, progress=progress, **kwargs)

    def is_dirty(self, path=None):
        return self.repo().is_dirty(path=path)


class _CloneProgress(object):

    def __init__(self, progress_reporter):
        try:
            updater = progress_reporter.update
        except AttributeError:
            raise TypeError("Progress reporter must have an 'update' method")
        else:
            if not callable(updater):
                raise TypeError("Progress reporter 'update' attribute does not appear to"
                        " be callable")
        self.pr = progress_reporter
        try:
            self.pr.unit = 'objects'
        except AttributeError:
            pass

        self._opcode = 0

    def __call__(self, op_code, cur_count, max_count=None, message=''):
        if op_code != self._opcode:
            self.pr.n = 0
        if max_count is not None:
            self.pr.total = max_count
        self.pr.update(cur_count - self.pr.n)
