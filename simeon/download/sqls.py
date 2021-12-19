"""
Module to process SQL files from edX
"""
import glob
import itertools
import os
import signal
import sys
import traceback
import zipfile
import multiprocessing as mp
from multiprocessing.pool import (
    Pool as ProcessPool, TimeoutError,
)

from simeon.download.utilities import (
    decrypt_files, format_sql_filename
)
from simeon.exceptions import (
    DecryptionError, EarlyExitError, SplitException
)


SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'upload', 'schemas'
)


proc_zfile = None


def _pool_initializer(fname):
    """
    Process pool initializer
    """
    global proc_zfile
    proc_zfile = zipfile.ZipFile(fname)
    def sighandler(sig, frame):
        raise EarlyExitError(
            'SQL bundle splitting interrupted prematurely'
        )
    sigs = [signal.SIGABRT, signal.SIGTERM, signal.SIGINT]
    for sig in sigs:
        signal.signal(sig, signal.SIG_DFL)


def _sum_batch_sizes(batch):
    """
    Get the sum of the file sizes in the given batch of file names
    """
    try:
        return sum(map(lambda f: os.stat(f).st_size, batch))
    except OSError:
        return 0


def _batch_by_dirs(dirnames, size):
    """
    Batch the .gpg files using the main course directories
    """
    bucket = []
    for dname in dirnames:
        files = glob.iglob(os.path.join(dname, '*.gpg'))
        files = itertools.chain(
            files, glob.iglob(os.path.join(dname, 'ora', '*.gpg'))
        )
        for file_ in files:
            bucket.append(file_)
            if len(bucket) >= size:
                yield bucket[:]
                bucket = []
    if bucket:
        yield bucket

        
def _batch_them(items, size):
    """
    Batch the given items by size.
    If the sum of the file sizes in a batch is greater than
    500 megabytes, then yield the batch even if it does not
    have as many as items as the given size parameter.
    """
    bucket = []
    for item in items:
        bucket.append(item)
        if len(bucket) >= size:
            yield bucket[:]
            bucket = []
    if bucket:
        yield bucket


def _batch_archive_names(names, size, include_edge=False):
    """
    Batch the file names inside the archive by size
    """
    bucket = []
    for name in names:
        if not include_edge and '-edge' in name:
            continue
        if name.endswith('/'):
            continue
        bucket.append(name)
        if len(bucket) == size:
            yield bucket[:]
            bucket = []
    if bucket:
        yield bucket


def force_delete_files(files, logger=None):
    """
    Delete the given files without regard for whatever or not they exist

    :type files: Iterable[str]
    :param files: Iterable of file names
    :type logger: Union[None, logging.Logger]
    :param logger: A logger object to log messages
    :rtype: None
    :return: Returns nothing, but deletes the given files from the local FS
    """
    for file_ in files:
        try:
            os.unlink(file_)
        except OSError as excp:
            if logger is not None:
                logger.warning(excp)
            continue


def batch_decrypt_files(
    all_files, size=100, verbose=False, logger=None,
    timeout=None, keepfiles=False, njobs=5,
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
    :type njobs: int
    :param njobs: Number of threads to use to call gpg in parallel
    :rtype: None
    :return: Nothing, but decrypts the .sql files from the given archive
    """
    failures = 0
    folders = set()
    orapth = os.path.join('', 'ora', '')
    for file_ in all_files:
        if orapth in file_:
            folders.add(os.path.dirname(os.path.dirname(file_)))
        else:
            folders.add(os.path.dirname(file_))
    while True:
        decryptions = 0
        for batch in _batch_by_dirs(folders, size):
            try:
                decrypt_files(
                    fnames=batch, verbose=verbose, logger=logger,
                    timeout=timeout, keepfiles=True
                )
                if not keepfiles:
                    force_delete_files(batch, logger=logger)
            except DecryptionError as excp:
                failures += 1
                if logger:
                    logger.error(excp)
            decryptions += len(batch)
        if decryptions == 0 or failures or keepfiles:
            break
    if failures:
        msg = (
            '{c} batches of {s} files each failed to decrypt. '
            'Please consult the logs'
        )
        raise DecryptionError(msg.format(c=failures, s=size))


def unpacker(fname, names, ddir, cpaths=None, tables_only=False):
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
        if target_dir.count('ora'):
            cfolder = os.path.basename(os.path.dirname(target_dir))
        else:
            cfolder = os.path.basename(target_dir)
        if cpaths and cfolder not in cpaths:
            continue
        if 'ccx' in cfolder:
            dir_segments = cfolder.replace('-', '__', 1).split('-')
            clean = '{f}__{s}'.format(
                f='_'.join(dir_segments[:-3]), s='_'.join(dir_segments[-3:])
            )
        else:
            clean = (
                '__'.join(cfolder.replace('-', '__', 1).rsplit('-', 1))
                .replace('-', '_')
            )
        clean = clean.replace('.', '_')
        target_dir = target_dir.replace(cfolder, clean)
        target_name = target_name.replace(cfolder, clean)
        if tables_only:
            targets.append(target_name)
            continue
        os.makedirs(target_dir, exist_ok=True)
        with proc_zfile.open(name) as zh, open(target_name, 'wb') as fh:
            while True:
                chunk = zh.read(1048576)
                if not chunk:
                    break
                fh.write(chunk)
        targets.append(target_name)
    return targets


def process_sql_archive(
    archive, ddir=None, include_edge=False, courses=None,
    size=5, tables_only=False, debug=False,
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
    :type tables_only: bool
    :param tables_only: Whether to extract file names only (no unarchiving)
    :type debug: bool
    :param debug: Show the stacktrace when an error occurs
    :rtype: Set[str]
    :return: A set of file names
    """
    cpaths = set()
    for c in (courses or []):
        cpaths.add(c.replace('/', '-'))
    if ddir is None:
        ddir, _ = os.path.split(archive)
    out = set()
    with zipfile.ZipFile(archive) as zf:
        names = zf.namelist()
    batches = _batch_archive_names(names, len(names) // size, include_edge)
    with ProcessPool(
        size, initializer=_pool_initializer,
        initargs=(archive,)
    ) as pool:
        results = []
        for batch in batches:
            results.append(pool.apply_async(
                unpacker, args=(archive, batch, ddir, cpaths, tables_only)
            ))
        processed = 0
        while processed < len(results):
            for result in results:
                try:
                    targets = result.get(timeout=60)
                    processed += 1
                    out.update(targets or [])
                except TimeoutError:
                    continue
                except KeyboardInterrupt:
                    raise SplitException(
                        'The SQL bundle unpacking was interrupted by '
                        'the user.'
                    )
                except:
                    _, excp, tb = sys.exc_info()
                    traces = ['{e}'.format(e=excp)]
                    if debug:
                        traces += map(str.strip, traceback.format_tb(tb))
                        excp = '\n'.join(traces)
                    msg = 'Failed to unpack items from archive {a}: {e}'
                    raise SplitException(msg.format(a=archive, e=excp))
    return out
