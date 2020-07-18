def hash_file(hsh, fname, blocksize=None):
    '''
    Updates the given hash object with the contents of a file.

    The file is read in `blocksize` chunks to avoid eating up too much memory at a time.

    Parameters
    ----------
    hsh : `hashlib.hash <hashlib>`
        The hash object to update
    fname : str
        The filename for the file to hash
    blocksize : int, optional
        The number of bytes to read at a time. If not provided, will use
        `hsh.block_size <hashlib.hash.block_size>` instead.
    '''
    if not blocksize:
        blocksize = hsh.block_size

    with open(fname, 'rb') as fh:
        while True:
            block = fh.read(blocksize)
            if not block:
                break
            hsh.update(block)
