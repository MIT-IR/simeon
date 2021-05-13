"""
Module to process SQL files from edX
"""
import os
import sys
import zipfile
from multiprocessing.pool import (
    Pool as ProcessPool, ThreadPool
)

from simeon.download.utilities import (
    decrypt_files, format_sql_filename
)
from simeon.exceptions import SplitException


proc_zfile = None


def _pool_initializer(fname):
    """
    Process pool initializer
    """
    global proc_zfile
    proc_zfile = zipfile.ZipFile(fname)


def _batch_them(items, size):
    """
    Batch the given items by size
    """
    bucket = []
    for item in items:
        if len(bucket) == size:
            yield bucket[:]
            bucket = []
        bucket.append(item)
    if bucket:
        yield bucket


def _batch_archive_names(names, size, include_edge=False, courses=None):
    """
    Batch the file names inside the archive by size
    """
    bucket = []
    for name in names:
        if not include_edge and '-edge' in name:
            continue
        if not courses:
            bucket.append(name)
            continue
        if any(c in name for c in courses):
            bucket.append(name)
        if len(bucket) == size:
            yield bucket[:]
            bucket = []
        bucket.append(name)
    if bucket:
        yield bucket


def _delete_all(items):
    """
    Delete the given items from the local file system
    """
    for item in items:
        try:
            os.remove(item)
        except:
            continue


def batch_decrypt_files(
    all_files, size=100, verbose=False, logger=None,
    timeout=None, keepfiles=False,
):
    """
    Batch the files by the given size and pass each batch to gpg to decrypt.

    :type all_files: List[str]
    :param all_files: List of file names
    :type size: int
    :param size: The batch size
    :type verbose: bool
    :param verbose: Print the command to be run
    :type logger: logging.Logger
    :param logger: A logging.Logger object to print the command with
    :type timeout: Union[int, None]
    :param timeout: Number of seconds to wait for the decryption to finish
    :type keepfiles: bool
    :param keepfiles: Keep the encrypted files after decrypting them.
    :rtype: None
    :return: Nothing
    """
    with ThreadPool(10) as pool:
        results = dict()
        for batch in _batch_them(all_files, size):
            async_result = pool.apply_async(
                    func=decrypt_files, kwds=dict(
                        fnames=batch, verbose=verbose,
                        logger=logger, timeout=timeout
                    )
            )
            results[async_result] = batch
        for result in results:
            result.get()
            if not keepfiles:
                _delete_all(results[result])


def unpacker(fname, names, ddir):
    """
    A worker callable to pass a Thread or Process pool
    """
    global proc_zfile
    targets = []
    for name in names:
        name, target_name = format_sql_filename(name)
        if name is None or target_name is None:
            continue
        target_name = os.path.join(ddir, target_name)
        target_dir = os.path.dirname(target_name)
        os.makedirs(target_dir, exist_ok=True)
        with proc_zfile.open(name) as zh, open(target_name, 'wb') as fh:
            for line in zh:
                fh.write(line)
        targets.append(target_name)
    return targets


def process_sql_archive(
    archive, ddir=None, include_edge=False, courses=None, size=10
):
    """
    Unpack and decrypt files inside the given archive

    :type archive: str
    :param archive: SQL data package (a ZIP archive)
    :type ddir: str
    :param ddir: The destination directory of the unpacked files
    :type include: bool
    :param include_edge: Include the files from the edge site
    :type courses: Union[Iterable[str], None]
    :param courses: A list of course IDs whose data files are unpacked
    :type size: int
    :param size: The size of the thread or process pool doing the unpacking
    :rtype: Iterable[str]
    :return: List of file names
    """
    courses = set(c.replace('/', '-') for c in (courses or []))
    if ddir is None:
        ddir, _ = os.path.split(archive)
    out = []
    with zipfile.ZipFile(archive) as zf:
        names = zf.namelist()
        batches = _batch_archive_names(
            names, len(names) // size,
            include_edge, courses
        )
        with ProcessPool(
            size, initializer=_pool_initializer,
            initargs=(archive,)
        ) as pool:
            results = []
            for batch in batches:
                results.append(
                    pool.apply_async(unpacker, args=(archive, batch, ddir))
                )
            for result in results:
                try:
                    result = result.get()
                except:
                    _, excp, _ = sys.exc_info()
                    msg = 'Failed to unpack items from archive {a}: {e}'
                    raise SplitException(
                        msg.format(a=archive, e=excp)
                    )
                if not result:
                    continue
                out.extend(result)
    return out
