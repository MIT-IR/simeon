"""
Module to process SQL files from edX
"""
import os
import zipfile
from multiprocessing.pool import ThreadPool

from simeon.download.utilities import (
    decrypt_files, format_sql_filename
)


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


def unpacker(zfile, name, ddir):
    """
    A worker callable to pass a Thread or Process pool
    """
    name, target_name = format_sql_filename(name)
    if name is None or target_name is None:
        return
    target_name = os.path.join(ddir, target_name)
    target_dir = os.path.dirname(target_name)
    os.makedirs(target_dir, exist_ok=True)
    with zfile.open(name) as zh, open(target_name, 'wb') as fh:
        for line in zh:
            fh.write(line)
    return target_name


def process_sql_archive(archive, ddir=None, include_edge=False):
    """
    Unpack and decrypt files inside the given archive

    :type archive: str
    :param archive: SQL data package (a ZIP archive)
    :type ddir: str
    :param ddir: The destination directory of the unpacked files
    :type include: bool
    :param include_edge: Include the files from the edge site
    :rtype: List[str]
    :return: List of file names
    """
    if ddir is None:
        ddir, _ = os.path.split(archive)
    out = []
    with zipfile.ZipFile(archive) as zf:
        if include_edge:
            names = zf.namelist()
        else:
            names = filter(lambda f: '-edge' not in f, zf.namelist())
        with ThreadPool(10) as pool:
            results = []
            for name in names:
                results.append(
                    pool.apply_async(unpacker, args=(zf, name, ddir))
                )
            for result in results:
                result = result.get()
                if not result:
                    continue
                out.append(result)
    return out
