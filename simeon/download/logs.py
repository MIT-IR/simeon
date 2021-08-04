"""
Module to process tracking log files from edX
"""
import errno
import gzip
import json
import os
import signal
import sys
import time
import traceback
from datetime import datetime
from json.decoder import JSONDecodeError
from multiprocessing.pool import (
    Pool, TimeoutError
)
from typing import Dict, List, Union

from dateutil.parser import parse as parse_date

from simeon.download import utilities as utils
from simeon.exceptions import (
    EarlyExitError, MissingSchemaException, SplitException,
)
from simeon.report import utilities as rutils


SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'upload', 'schemas'
)


def _process_initializer():
    """
    Process initializer for multiprocessing pools
    """
    def sighandler(sig, frame):
        raise EarlyExitError(
            'Tracking log splitting interrupted prematurely'
        )
    sigs = [signal.SIGABRT, signal.SIGTERM, signal.SIGINT]
    for sig in sigs:
        signal.signal(sig, signal.SIG_DFL)


def _cleanup_handles(fhandles, sleep=1):
    """
    Flush and close the given file handles.
    Sleep sleep number of seconds if necessary,
    so the OS can reclaim the file descriptors
    associated with the given handles.
    """
    for fhandle in fhandles:
        fhandle.flush()
        os.fsync(fhandle.fileno())
        os.close(fhandle.fileno())
    if sleep:
        time.sleep(sleep)


# pylint:disable=unsubscriptable-object
def process_line(
    line: Union[str, bytes], lcount: int,
    date: Union[None, datetime]=None, is_gzip=True,
    courses: List[str]=None
) -> dict:
    """
    Process the line from a tracking log file and return the reformatted
    line (deserialized) along with the name of its destination file.

    :type line: Union[str, bytes]
    :param line: A line from the tracking logs
    :type lcount: int
    :param lcount: The line number of the given line
    :type date: Union[None, datetime]
    :param date: The date of the file where this line comes from.
    :type is_gzip: bool
    :param is_gzip: Whether or not this line came from a GZIP file
    :type courses: Union[Iterable[str], None]
    :param courses: A list of course IDs whose records are exported
    :rtype: Dict[str, Union[Dict[str, str], str]]
    :return: Dictionary with both the data and its destination file name
    """
    line = line.strip()
    if isinstance(line, bytes):
        line = line.decode('utf8', 'ignore')
    if not line.startswith('{'):
        if 'localhost {' in line[:27]:
            line = line.split('localhost ')[-1]
    try:
        record = json.loads(line)
    except (JSONDecodeError, TypeError):
        return {
            'data': line,
            'filename': os.path.join(
                'dead_letters',
                'dead_letter_queue_{p}.json.gz'.format(p=os.getpid())
            )
        }
    if not isinstance(record.get('event'), dict):
        try:
            record['event'] = json.loads(record.get('event', '{}'))
        except (JSONDecodeError, TypeError):
            record['event'] = {'event': record['event']}
    course_id = utils.get_course_id(record)
    if courses and course_id not in courses:
        return {}
    record['course_id'] = course_id
    try:
        utils.rephrase_record(record)
    except KeyError:
        return {
            'data': line,
            'filename': os.path.join(
                'dead_letters',
                'dead_letter_queue_{p}.json.gz'.format(p=os.getpid())
            )
        }
    if any(k not in record for k in ('event', 'event_type')):
        return {
            'data': line,
            'filename': os.path.join(
                'dead_letters',
                'dead_letter_queue_{p}.json.gz'.format(p=os.getpid())
            )
        }
    if not date:
        try:
            date = parse_date(record.get('time', ''))
            outfile = utils.make_tracklog_path(
                course_id, date.strftime('%Y-%m-%d'), is_gzip
            )
        except Exception:
            ext = '.gz' if is_gzip else ''
            outfile = os.path.join(
                course_id.replace('.', '_').replace('/', '__') or 'UNKNOWN',
                'tracklog-unknown.json{x}'.format(x=ext)
            )
    else:
        outfile = utils.make_tracklog_path(
            course_id, date.strftime('%Y-%m-%d'), is_gzip
        )
    return {'data': record, 'filename': outfile}


# pylint:enable=unsubscriptable-object
def split_tracking_log(
    filename: str, ddir: str, dynamic_date: bool=False,
    courses: List[str]=None,
    schema_dir=SCHEMA_DIR,
):
    """
    Split the records in the given GZIP tracking log file.
    This function is very resource hungry because it keeps around
    a lot of open file handles and writes to them whenever it processes
    a good record. Some attempts are made to keep records around whenever
    the process is no longer allowed to open new files. But that will likely
    lead to the exhaustion of the running process's alotted memory.

    :NOTE: If you've got a better way, please update me.

    :type filename: str
    :param filename: The GZIP file to split
    :type ddir: str
    :param ddir: Destination directory of the generated file
    :type dynamic_date: bool
    :param dynamic_date: Use dates from the JSON records to make
        output file names
    :type courses: Union[Iterable[str], None]
    :param courses: A list of course IDs whose records are exported
    :type schema_dir: Union[None, str]
    :param schema_dir: Directory where to find schema files
    :rtype: bool
    :return: True if files have been generated. False, otherwise
    """
    schema_dir = schema_dir or SCHEMA_DIR
    schema_file = os.path.join(
        schema_dir, 'schema_tracking_log.json'
    )
    if not os.path.exists(schema_file):
        raise MissingSchemaException(
            'The schema file {f} does not exist. '
            'Please provide a valid schema directory'.format(f=schema_file)
        )
    with open(schema_file) as sfh:
        schema = json.load(sfh).get('tracking_log')
    if not isinstance(courses, set):
        courses = set(c for c in (courses or []))
    fhandles = dict()
    if not dynamic_date:
        date = utils.get_file_date(filename)
    else:
        date = None
    with gzip.open(filename) as zfh:
        stragglers = []
        for i, line in enumerate(zfh):
            line_info = process_line(line, i + 1, date=date, courses=courses)
            if not line_info:
                continue
            data = line_info['data']
            user_id = (data.get('context') or {}).get('user_id')
            username = data.get('username')
            if not any([user_id, username]):
                continue
            if isinstance(data, dict):
                if courses and data.get('course_id') not in courses:
                    continue
            fname = line_info.get('filename')
            fname = os.path.join(ddir, fname)
            if fname not in fhandles:
                try:
                    fhandles[fname] = utils.make_file_handle(
                        fname, is_gzip=True
                    )
                except OSError as excp:
                    if excp.errno == errno.EMFILE:
                       stragglers.append(line_info)
                       continue
                    if excp.errno == errno.ENAMETOOLONG:
                        continue
                    raise excp
            fhandle = fhandles[fname]
            if not isinstance(data, str):
                rutils.check_record_schema(data, schema)
                rutils.drop_extra_keys(data, schema)
                data = json.dumps(data)
            if isinstance(fhandle, gzip.GzipFile):
                fhandle.write(data.encode('utf8', 'ignore') + b'\n')
            else:
                fhandle.write(data + '\n')
        if not stragglers:
            return bool(fhandles)
        # Working around EMFILE errors
        _cleanup_handles(fhandles.values())
        # Sort the stragglers by file name and use a file tracker
        stragglers = sorted(stragglers, key=lambda s: s.get('filename'))
        pfname = None
        while stragglers:
            try:
                rec = stragglers.pop()
                fname = rec.get('filename')
                fname = os.path.join(ddir, fname)
                if pfname and pfname != fname:
                    try:
                        _cleanup_handles([fhandles[pfname]], None)
                    except OSError:
                        pass
                data = line_info['data']
                if fname not in fhandles:
                    try:
                        fhandle = utils.make_file_handle(fname, is_gzip=True)
                        fhandles[fname] = fhandle
                    except OSError as excp:
                        if excp.errno == errno.EMFILE:
                            stragglers.append(rec)
                            continue
                        if excp.errno == errno.ENAMETOOLONG:
                            continue
                        raise excp
                if not isinstance(data, str):
                    rutils.check_record_schema(data, schema)
                    rutils.drop_extra_keys(data, schema)
                    data = json.dumps(data)
                if isinstance(fhandle, gzip.GzipFile):
                    fhandle.write(data.encode('utf8', 'ignore') + b'\n')
                else:
                    fhandle.write(data + '\n')
                pfname = fname
            except IndexError:
                break
    return bool(fhandles)


def batch_split_tracking_logs(
    filenames, ddir, dynamic_date=False,
    courses=None, verbose=True, logger=None,
    size=10, schema_dir=SCHEMA_DIR, debug=False,
):
    """
    Call split_tracking_log on each file inside a process or thread pool
    """
    schema_dir = schema_dir or SCHEMA_DIR
    if not size or size > len(filenames):
        size = len(filenames)
    splits = 0
    processed = 0
    with Pool(size, initializer=_process_initializer) as pool:
        results = dict()
        for fname in filenames:
            if verbose and logger:
                logger.info('Splitting {f}'.format(f=fname))
            result = pool.apply_async(
                func=split_tracking_log,
                kwds=dict(
                    filename=fname, ddir=ddir,
                    dynamic_date=dynamic_date, courses=courses,
                )
            )
            results[fname] = (result, False)
        while processed < len(filenames):
            for fname in results:
                result, done = results[fname]
                if done:
                    continue
                try:
                    # Do the rounds every minute and check if a worker
                    # is done with its job
                    rc = result.get(timeout=60)
                    splits += rc
                    results[fname] = (result, True)
                    processed += 1
                    if rc:
                        if verbose and logger:
                            logger.info('Done splitting {f}'.format(f=fname))
                        continue
                    if logger:
                        errmsg = (
                            'No files were extracted while splitting the tracking '
                            'log file {f!r} with the given criteria. Moving on...'
                        )
                        logger.warning(errmsg.format(f=fname))
                        logger.warning('Done splitting {f}'.format(f=fname))
                except TimeoutError:
                    continue
                except KeyboardInterrupt:
                    msg = 'Failed to split {f}: Interrupted by the user'
                    logger.error(msg.format(f=fname))
                    return False
                except:
                    _, excp, tb = sys.exc_info()
                    msg = 'Failed to split {f}{e}'
                    exit_excepts = (EarlyExitError, SystemExit)
                    if isinstance(excp, exit_excepts):
                        raise SplitException(
                            msg.format(f=fname, e='')
                        )
                    else:
                        if debug:
                            traces = [': {e}'.format(e=excp)]
                            traces += map(str.strip, traceback.format_tb(tb))
                            excp_str = '\n'.join(traces)
                        logger.error(msg.format(f=fname, e=excp_str))
                    results[fname] = (result, True)
                    processed += 1
    return splits == len(filenames)
