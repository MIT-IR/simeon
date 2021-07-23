"""
Module to process SQL files from edX
"""
import os
import signal
import sys
import traceback
import zipfile
from multiprocessing.pool import (
    Pool as ProcessPool, ThreadPool, TimeoutError
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


def _batch_them(items, size):
    """
    Batch the given items by size
    """
    bucket = []
    for item in items:
        bucket.append(item)
        if len(bucket) == size:
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
    :return: Nothing, but decrypts the .sql files from the given archive
    """
    with ThreadPool(5) as pool:
        results = dict()
        for batch in _batch_them(all_files, size):
            async_result = pool.apply_async(
                    func=decrypt_files, kwds=dict(
                        fnames=batch, verbose=verbose,
                        logger=logger, timeout=timeout
                    )
            )
            results[async_result] = batch
        failures = 0
        for result in results:
            try:
                result.get()
            except DecryptionError as excp:
                failures += 1
                if logger:
                    logger.error(excp)
    if failures:
        raise DecryptionError(
            'Multiple files failed to decrypt. Please consult the logs.'
        )
    if not keepfiles:
        _delete_all(all_files)


def unpacker(fname, names, ddir, courses=None, tables_only=False):
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
        if courses and cfolder not in courses:
            continue
        if tables_only:
            targets.append(target_name)
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
        os.makedirs(target_dir, exist_ok=True)
        with proc_zfile.open(name) as zh, open(target_name, 'wb') as fh:
            for line in zh:
                fh.write(line)
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
    :rtype: Iterable[str]
    :return: List of file names
    """
    cpaths = set()
    for c in (courses or []):
        cpaths.add(c.replace('/', '-'))
    if ddir is None:
        ddir, _ = os.path.split(archive)
    out = []
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
                    out.extend(targets or [])
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
