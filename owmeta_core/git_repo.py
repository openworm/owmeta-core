from os.path import exists

from git import Repo


class GitRepoProvider(object):

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

    def reset(self, paths=None, working_tree=True):
        from git.refs.head import HEAD
        repo = self.repo()
        head = HEAD(repo)
        if paths and working_tree:
            head.reset(paths=paths)
            paths = [p for p in paths if exists(p)]
            repo.index.checkout(paths=paths, force=True)

    def commit(self, msg):
        self.repo().index.commit(msg)

    def repo(self):
        if self._repo is None:
            self._repo = Repo(self.base)
        return self._repo

    def clone(self, url, base, progress=None, **kwargs):
        if progress is not None:
            progress = _CloneProgress(progress)
        Repo.clone_from(url, base, progress=progress, **kwargs)

    def is_dirty(self, path=None):
        return self.repo().is_dirty(path=path)


class _CloneProgress(object):

    def __init__(self, progress_reporter):
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
