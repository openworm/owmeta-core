from queue import Empty
from multiprocessing import Process, Queue, Semaphore
from owmeta_core.file_lock import lock_file, InvalidLockAccess
from os import unlink
from os.path import join as p

import pytest


def mutex_test_f(v, parent_q, fname, done, wait):
    with lock_file(fname):
        parent_q.put(v, True)
        done.release()
        wait.acquire()


@pytest.mark.inttest
def test_mutex(lock_fname):
    '''
    Here we have two process, each tries to acquire the lock, wake up the main thread, and
    then wait on the "wait" semaphore. When control is returned to the main thread which
    then checks that the queue only contains one entry: if it contained two entries that
    would mean the lock file does not guarantee mutual exclusion.
    '''
    q = Queue()
    done = Semaphore(0)
    wait = Semaphore(0)
    p1 = Process(target=mutex_test_f, args=(1, q, lock_fname, done, wait))
    p2 = Process(target=mutex_test_f, args=(2, q, lock_fname, done, wait))
    p1.start()
    p2.start()
    assert done.acquire(timeout=1), 'Neither process could acquire the lock file?'
    done.release()
    try:
        print(q.get(timeout=1))
        with pytest.raises(Empty):
            print(q.get(timeout=1))
    finally:
        wait.release()
        wait.release()
        p1.join(1)
        assert p1.exitcode is not None, "Process 1 failed to exit in time"
        p2.join(1)
        assert p2.exitcode is not None, "Process 2 failed to exit in time"


@pytest.mark.inttest
def test_remove_lock_file(lock_fname):
    '''
    Unlinking the lock file early is not allowed generally and probably indicates a logic
    error, so we give an exception when "releasing" in that case

    Note: there *are* ways to prevent accidental deletion, but they we don't deal with
    that. Accidentally releasing the lock when you don't hold it, however, does cause an
    error (see below)
    '''
    lock = lock_file(lock_fname)
    lock.acquire()
    unlink(lock_fname)
    with pytest.raises(FileNotFoundError):
        lock.release()


@pytest.mark.inttest
def test_acquire_dangling_lock(lock_fname):
    lock_file(lock_fname, unique_key='test').acquire()
    lock_file(lock_fname, unique_key='test').acquire()


@pytest.mark.inttest
def test_early_release(lock_fname):
    '''
    Releasing the lock early is not allowed
    '''
    lock = lock_file(lock_fname)
    lock.acquire()
    lock.release()

    with pytest.raises(InvalidLockAccess):
        lock.release()


@pytest.mark.inttest
def test_release_without_acquire(lock_fname):
    '''
    Similar to the test above, but doesn't depend on the lock release "cleanup" behavior
    '''
    lock = lock_file(lock_fname)

    with pytest.raises(InvalidLockAccess):
        lock.release()


@pytest.mark.inttest
def test_try_acquire(lock_fname):
    assert lock_file(lock_fname).try_acquire()
    assert not lock_file(lock_fname).try_acquire()


@pytest.mark.inttest
def test_acquire_twice_fails(lock_fname):
    lock = lock_file(lock_fname)
    assert lock.acquire()
    with pytest.raises(InvalidLockAccess):
        lock.acquire()


@pytest.fixture
def lock_fname(tempdir):
    yield p(tempdir, 'lock')
