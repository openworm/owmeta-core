from time import sleep
import os
from errno import EEXIST, ENOENT
import random


class lock_file(object):
    def __init__(self, fname, unique_key=None, wait_interval=.01):
        '''
        Parameters
        ----------
        fname : str
            The lock file
        unique_key : str or bytes
            A key for the lock request. This can be ommitted, but in that case, the lock
            will not be tolerant to process failures because you cannot restart a process
            with the same key to release the lock.
        wait_interval : int or float
            How long to wait between attempts to grab the lock
        '''
        if not unique_key:
            self._name = bytes(random.randrange(32, 127) for _ in range(10))
        else:
            self._name = unique_key if isinstance(unique_key, bytes) else unique_key.encode('UTF-8')

        self.fname = fname
        self.wait_interval = wait_interval
        self._have_lock = False
        self.released = None

    def __enter__(self):
        self._acq_ll()
        return self

    def acquire(self):
        return self._acq_ll()

    def try_acquire(self):
        return self._acq_ll(False)

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def _acq_ll(self, block=True):
        if self._have_lock:
            raise InvalidLockAccess(f'{self.fname} was already acquired')
        have_lock = False
        self.released = False
        while not have_lock:
            try:
                fd = os.open(self.fname, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            except OSError as oserr:
                if oserr.errno != EEXIST:
                    raise
                try:
                    with open(self.fname, 'rb') as f:
                        if f.read(len(self._name)) == self._name:
                            have_lock = True
                            continue
                except IOError as e:
                    if e.errno != ENOENT:
                        raise
                if not block:
                    break
                sleep(self.wait_interval)
            else:
                os.write(fd, self._name)
                os.fsync(fd)
                os.close(fd)
                have_lock = True
            if not block:
                break
        self._have_lock = have_lock
        return have_lock

    def release(self):
        if not self._have_lock:
            raise InvalidLockAccess(f'Attempted to release {self.fname} before it was'
                    ' acquired')
        if not self.released:
            try:
                os.unlink(self.fname)
                self._have_lock = False
            except FileNotFoundError:
                self._have_lock = False
                raise
            # If we fail above, we no longer have the lock, but we haven't really
            # "released" the lock, so we leave the value as False. At this point, this
            # variable only provides diagnostic info since we would only set _have_lock
            # again when we actually do have the lock.
            self.released = True


class InvalidLockAccess(Exception):
    '''
    Raised when attempt to do something improper with a lock like releasing the lock when
    you haven't yet acquired it.
    '''
