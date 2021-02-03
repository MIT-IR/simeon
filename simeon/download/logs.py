"""
Module to process tracking log files from edX
"""
import gzip
import json
import os
from datetime import datetime
from typing import Dict, List, Union

from dateutil.parser import parse as parse_date

import simeon.download.utilities as utils



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
    except json.decoder.JSONDecodeError:
        return {'data': line, 'filename': 'dead_letter_queue.json.gz'}
    course_id = utils.get_course_id(record)
    record['course_id'] = course_id
    try:
        utils.rephrase_record(record)
    except KeyError:
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
    :type courses: Union[List[str], None]
    :param courses: A list of course IDs whose records are exported
    :rtype: None
    :return: Writes records to generated file names
    """
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
            if isinstance(data, dict):
                if courses and data.get('course_id') not in courses:
                    continue
            fname = line_info.get('filename')
            fname = os.path.join(ddir, fname)
            if fname not in fhandles:
                fhandles[fname] = utils.make_file_handle(fname, is_gzip=True)
            fhandle = fhandles[fname]
            if not isinstance(data, str):
                data = json.dumps(data)
            if isinstance(fhandle, gzip.GzipFile):
                fhandle.write(data.encode('utf8', 'ignore') + b'\n')
            else:
                fhandle.write(data + '\n')
