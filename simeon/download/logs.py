"""
Module to process tracking log files from edX
"""
import gzip
import json
import os
import sys
import traceback
from datetime import datetime
from json.decoder import JSONDecodeError
from multiprocessing.pool import (
    Pool, TimeoutError
)
from typing import Dict, List, Union

from dateutil.parser import parse as parse_date

import simeon.download.utilities as utils
from simeon.report.utilities import SCHEMA_DIR, drop_extra_keys


# pylint:disable=unsubscriptable-object
def process_line(
    line: Union[str, bytes], lcount: int,
    date: Union[None, datetime]=None, is_gzip=True
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
    :rtype: Dict[str, Union[Dict[str, str], str]]
    :return: Dictionary with both the data and its destination file name
    """
    line = line.strip()
    if isinstance(line, bytes):
        line = line.decode('utf8', 'ignore')
    if not line.startswith('{'):
        if 'localhost {' in line[:27]:
            line = line[26:]
    try:
        record = json.loads(line)
    except (JSONDecodeError, TypeError):
        return {'data': line, 'filename': 'dead_letter_queue.json.gz'}
    if not isinstance(record.get('event'), dict):
        try:
            record['event'] = json.loads(record.get('event', '{}'))
        except (JSONDecodeError, TypeError):
            record['event'] = {'event': record['event']}
    course_id = utils.get_course_id(record)
    record['course_id'] = course_id
    try:
        utils.rephrase_record(record)
    except KeyError:
        return {'data': line, 'filename': 'dead_letter_queue.json.gz'}
    if any(k not in record for k in ('event', 'event_type')):
        return {'data': line, 'filename': 'dead_letter_queue.json.gz'}
    if not date:
        try:
            date = parse_date(record.get('time', ''))
            outfile = utils.make_tracklog_path(
                course_id, date.strftime('%Y-%m-%d'), is_gzip
            )
        except Exception:
            ext = '.gz' if is_gzip else ''
            outfile = os.path.join(
                course_id.replace('.', '_').replace('/', '__'),
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
):
    """
    Split the records in the given GZIP tracking log file

    :type filename: str
    :param filename: The GZIP file to split
    :type ddir: str
    :param ddir: Destination directory of the generated file
    :type dynamic_date: bool
    :param dynamic_date: Use dates from the JSON records to make
        output file names
    :type courses: Union[Iterable[str], None]
    :param courses: A list of course IDs whose records are exported
    :rtype: bool
    :return: True if files have generated. False, otherwise
    """
    schema_file = os.path.join(
        SCHEMA_DIR, 'schema_tracking_log.json'
    )
    with open(schema_file) as sfh:
        schema = json.load(sfh).get('tracking_log')
    courses = set(courses) if courses else set()
    fhandles = dict()
    if not dynamic_date:
        date = utils.get_file_date(filename)
    else:
        date = None
    with gzip.open(filename) as zfh:
        for i, line in enumerate(zfh):
            line_info = process_line(line, i + 1, date=date)
            data = line_info['data']
            user_id = (data.get('context') or {}).get('user_id')
            username = data.get('username')
            if not all([user_id, username]):
                continue
            if isinstance(data, dict):
                if courses and data.get('course_id') not in courses:
                    continue
            fname = line_info.get('filename')
            fname = os.path.join(ddir, fname)
            if fname not in fhandles:
                fhandles[fname] = utils.make_file_handle(fname, is_gzip=True)
            fhandle = fhandles[fname]
            if not isinstance(data, str):
                drop_extra_keys(data, schema)
                data = json.dumps(data)
            if isinstance(fhandle, gzip.GzipFile):
                fhandle.write(data.encode('utf8', 'ignore') + b'\n')
            else:
                fhandle.write(data + '\n')
    return bool(fhandles)


def batch_split_tracking_logs(
    filenames, ddir, dynamic_date=False,
    courses=None, verbose=True, logger=None,
):
    """
    Call split_tracking_log on each file inside a process or thread pool
    """
    size = 5 if len(filenames) >= 10 else 2
    splits = 0
    processed = 0
    with Pool(size) as pool:
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
                    rc = result.get(timeout=1)
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
                        logger.warn(errmsg.format(f=fname))
                        logger.warn('Done splitting {f}'.format(f=fname))
                except TimeoutError:
                    continue
                except:
                    results[fname] = (result, True)
                    processed += 1
                    _, excp, tb = sys.exc_info()
                    msg = 'Failed to split {f}: {e}'
                    if verbose:
                        traces = ['{e}'.format(e=excp)]
                        traces += map(str.strip, traceback.format_tb(tb))
                        msg = msg.format(f=fname, e='\n'.join(traces))
                    else:
                        msg = msg.format(f=fname, e=excp)
                    logger.error(msg)
    return splits == len(filenames)
