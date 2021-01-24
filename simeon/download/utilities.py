"""
Some utility functions
"""
import itertools as its
import gzip
import json
import os
import re
import urllib.parse as urlparser
from datetime import datetime
from functools import lru_cache

from dateutil.parser import parse as parse_date


CID_PATT1 = re.compile(r'^http[s]*://[^/]+/courses/([^/]+/[^/]+/[^/]+)/', re.I)
CID_PATT2 = re.compile(r'/courses/([^/]+/[^/]+/[^/]+)/', re.I)


def make_file_handle(fname: str, mode: str='w', is_gzip: bool=False):
    """
    Create a file handle pointing the given file name.
    If the directory of the file does not exist, create it.

    :type fname: str
    :param fname: A file name whose handle needs to be created.
    :type mode: str
    :param mode: "a[b]?" for append or "w[b]?" for write
    :type is_gzip: bool
    :param is_gzip: Whether or not to open it as a gzip file handle
    :rtype: Union[TextIOWrapper, BufferedReader]
    """
    fname = os.path.expanduser(fname)
    dirname, _ = os.path.split(fname)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    if is_gzip:
        return gzip.open(fname, mode)
    return open(fname, mode)


def get_course_id(record: dict, org_keywords=('mit', 'vj')) -> str:
    """
    Given a JSON record, try getting the course_id out of it.

    :type record: dict
    :param record: A deserialized JSON record
    :rtype: str
    :return: A valid edX course ID or an empty string
    """
    course = record.get(
        'course_id',
        record.get('context', {}).get('course_id', '')
    )
    if not course:
        if 'browser' in record.get('event_source', '').lower():
            course = urlparser.urlparse(record.get('page')).path
        else:
            course = urlparser.urlparse(record.get('event_type', '')).path
    chunks = its.dropwhile(
        lambda c: not c.strip(),
        course.replace('courses', '').strip('/').split(':')
    )
    course = next(chunks, '')
    if not any(k in course.lower() for k in org_keywords):
        course = next(chunks, '')
    return '/'.join(course.split('+')[:3])


@lru_cache(maxsize=None)
def make_tracklog_path(course_id: str, datestr: str, is_gzip=True) -> str:
    """
    Make a local file path name with the given course ID and datetime object

    :type course_id: str
    :param course_id: Properly formatted edX course ID
    :type datestr: str
    :param datestr: %Y-%m-%d formatted date associated with the tracking log
    :type is_gzip: bool
    :param is_gzip: Whether or not we're making a GZIP file path
    :rtype: str
    :return: A local FS file path
    """
    ext = '.gz' if is_gzip else ''
    return os.path.join(
        course_id.replace('.', '_').replace('/', '__'),
        'tracklog-{ds}.json{x}'.format(ds=datestr, x=ext)
    )


@lru_cache(maxsize=None)
def parse_mongo_tstamp(timestamp: str):
    """
    Try converting a MongoDB timestamp into a stringified datetime

    :type timestamp: str
    :param timestamp: String representing a timestamp
    :rtype: str
    :return: A formatted datetime
    """
    if not timestamp:
        return ''
    try:
        timestamp = int(timestamp)
    except (TypeError, ValueError):
        try:
            return str(parse_date(timestamp))
        except Exception:
            try:
                return str(parse_date(timestamp[:16]))
            except Exception:
                msg = '{t} is not a valid timestamp'.format(t=timestamp)
                raise ValueError(msg) from None
    try:
        return str(datetime.fromtimestamp(timestamp/1000.0))
    except Exception:
        msg = '{t} is not a valid timestamp'.format(t=timestamp)
        raise ValueError(msg) from None


def check_for_funny_keys(entry, name='toplevel'):
    for key, val in entry.items():
        if key.startswith('i4x-') or key.startswith('xblock.'):
            return True
        if key[0] in '0123456789':
            return True
        if '-' in key or '.' in key:
            newkey = key.replace('-','_').replace('.','__')
            entry[newkey] = val
            entry.pop(key)
            key = newkey
        if isinstance(val, dict):
            ret = check_for_funny_keys(val, name + '/' + key)
            if ret:
                entry[key] = json.dumps(val)
    return False


def rephrase_mongo_keys(record: dict):
    """
    Update the given record in place. The target keys
    are those whose values came from a MongoDB export

    For now, I am skipping Ike's check_for_funny_keys function
    from the ephrase_forum_data.py module in edx2bigquery

    :type record: dict
    :param record: A deserialized JSON record
    :rtype: None
    :return: Nothing
    """
    date_keys = (
        ('endorsement', 'time'), ('updated_at', '$date'),
        ('created_at', '$date'), ('last_activity_at', '$date')
    )
    str_keys = (
        ('historical_abuse_flaggers', None),
        ('abuse_flaggers', None),
        ('at_position_list', None),
        ('tags_array', None),
        ('up', 'votes'),
        ('up', 'votes')
    )
    if '_id' in record:
        record['mongoid'] = record['_id']['$oid']
        record.pop('_id')
    if 'parent_id' in record:
        data['parent_id'] = record['parent_id']['$oid']
    for key, subkey in date_keys:
        if key in record:
            record[key] = parse_mongo_tstamp(record[key][subkey])
    if 'comment_thread_id' in record:
        record['comment_thread_id'] = record['comment_thread_id']['$oid']
    endorsement_conds = (
        'endorsement' in record,
        record.get('endorsement', '') in ('null', None, ''),
    )
    if all(endorsement_conds):
        record.pop('endorsement')
    if 'parent_ids' in record:
        record['parent_ids'] = ' '.join(
            [subrec['$oid'] for subrec in record['parent_ids']]
        )
    for key, subkey in str_keys:
        if key in record:
            if subkey:
                subrec = record.get(subkey, {})
                subrec[key] = str(subrec.get(key, ''))
            else:
                record[key] = str(record.get(key, ''))
    check_for_funny_keys(record)
    for key in ('depth', 'retired_username'):
        record.pop(key, None)
