"""
Some utility functions for working with the downloaded data files
"""
import itertools as its
import gzip
import json
import math
import os
import re
import subprocess as sb
import urllib.parse as urlparser
from datetime import datetime
from functools import lru_cache

from dateutil.parser import parse as parse_date

from simeon.exceptions import DecryptionError


COURSE_PATHS = [
    ('page',), ('event_type',), ('context', 'path'), ('name',)
]
MODULE_PATHS = [
    ('context', 'module', 'usage_key'), ('page',),
    ('context', 'path'), ('event', 'event_type'),
    ('name',), ('event', 'id'), ('event', 'problem_id'),
    ('event_type',)
]
SQL_FILE_EXTS = {
    '.failed': 3,
    '.gz': 3,
    '.json': 3,
    '.mongo': 1,
    '.sql': 3,
}


def _extract_values(record, paths):
    """
    Given a list of JSON paths (tuples), extract
    all the values associated with the given paths
    """
    for path in paths:
        subrec = record or {}
        start = path[:-1]
        end = path[-1]
        for k in start:
            subrec = record.get(k, {}) or {}
        if not isinstance(subrec, dict):
            continue
        yield subrec.get(end, '')


def decrypt_files(fnames, verbose=True, logger=None, timeout=None):
    """
    Decrypt the given file with gpg.
    This assumes that the gpg command
    is available in the SHELL running this script.

    :type fnames: Union[str, List]
    :param fnames: A file name or a list of file names to decrypt
    :type verbose: bool
    :param verbose: Print the command to be run
    :type logger: logging.Logger
    :param logger: A logging.Logger object to print the command with
    :type timeout: Union[int, None]
    :param timeout: Number of seconds to wait for the decryption to finish
    :rtype: bool
    :return: Returns True if the decryption does not fail
    :raises: DecryptionError
    """
    if isinstance(fnames, str):
        fnames = [fnames]
    verbosity = '--verbose' if verbose else ''
    cmd = 'gpg {v} --batch --yes --decrypt-files {f}'.format(
        f=' '.join(fnames), v=verbosity
    )
    if verbose and logger is not None:
        logger.info('{m}...'.format(m=cmd[:200]))
    proc =  sb.Popen(cmd.split(), stdout=sb.PIPE, stderr=sb.PIPE)
    if proc.wait(timeout=timeout) != 0:
        err = proc.stderr.read().decode('utf8', 'ignore').strip()
        msg = 'Failed to decrypt file names {f} with return code {rc}: {e}'
        raise DecryptionError(
            msg.format(f=' '.join(fnames), e=err, rc=proc.returncode)
        )
    if verbose and logger is not None:
        for line in proc.stdout:
            logger.info(line.decode('utf8', 'ignore').strip())
    return True


def get_file_date(fname):
    """
    Extract the date in a file name and parse it into a datetime object

    :type fname: str
    :param fname: Some file name
    :rtype: Union[None, datetime]
    :return: Returns a datetime object or None
    """
    fname = os.path.basename(fname)
    try:
        return parse_date(re.search(r'\d{4}-\d{2}-\d{2}', fname).group(0))
    except:
        return None


def make_file_handle(fname: str, mode: str='wt', is_gzip: bool=False):
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


@lru_cache(maxsize=None)
def get_sql_course_id(course_str: str) -> str:
    """
    Given a course ID string from the SQL files,
    pluck out of the actual course ID and format it as follows:
    ORG/COURSE_NUMBER/TERM

    :type course_str: str
    :param course_str: The course ID string from edX
    :rtype: str
    :return: Actual course ID and format it properly
    """
    return course_str.split(':')[-1].replace('+', '/')


def format_sql_filename(fname: str) -> (str, str):
    """
    Reformat the given edX SQL encrypted file name into a name indicative
    of where the file should end up after the SQL archive is unpacked.
    site/folder/filename.ext.gext
    """
    if fname.endswith('/'):
        return None, None
    file_ = fname.replace('prod-edge', 'edge').replace('ora/', '')
    if fname.endswith('.gpg'):
        file_, _ = os.path.splitext(file_)
    dirname, bname = os.path.split(file_)
    _, ext = os.path.splitext(bname)
    limit = SQL_FILE_EXTS.get(ext)
    if limit is None:
        raise ValueError(
            '{f} has an expected extension. Expected are {x}'.format(
                f=fname, x=', '.join(SQL_FILE_EXTS)
            )
        )
    components = bname.rsplit('-', limit)
    if '.mongo' in bname:
        cid, out = components
        site, out, ending = out.replace('.mongo', ''), 'forum.mongo.gpg', ''
    else:
        cid, out, site, ending = components
    out = '{o}-{e}.gpg'.format(o=out, e=ending) if ending else out
    if 'ora/' in fname:
        out = os.path.join('ora', out)
    return (
        fname,
        os.path.join(
            site, dirname,
            cid,
            # cid.replace('-', '__', 2).replace('-', '_').replace('.', '_'),
            out,
        )
    )


def get_course_id(record: dict, paths=COURSE_PATHS) -> str:
    """
    Given a JSON record, try getting the course_id out of it.

    :type record: dict
    :param record: A deserialized JSON record
    :type paths: Iterable[Iterable[str]]
    :param paths: Paths to follow to find a matching course ID string
    :rtype: str
    :return: A valid edX course ID or an empty string
    """
    course_id = record.get('course_id')
    if not course_id:
        course_id = record.get('context', {}).get('course_id') or ''
    if not course_id:
        for course_id in _extract_values(record, paths):
            course_id = (course_id or '')
            if not (course_id.count('/') or course_id.count('+')):
                continue
            course_id = urlparser.urlparse(course_id).path
            if course_id:
                break
    course_id = (course_id or '').split('courses/')[-1]
    if course_id.count('i4x:') or course_id.count('data:image'):
        segments = '/'.join(course_id.split(':', 1)[0].split('+')[:3])
    else:
        segments = '/'.join(course_id.split(':', 1)[-1].split('+')[:3])
    return '/'.join([s for s in segments.rstrip('/').split('/') if s][:3])


def get_module_id(record: dict, paths=MODULE_PATHS):
    """
    Get the module ID of the given record

    :type record: dict
    :param record: A deserialized JSON record
    :type paths: Iterable[Iterable[str]]
    :param paths: Paths to follow to find a matching course ID string
    :rtype: str
    :return: A valid edX course ID or an empty string
    """
    values = _extract_values(record, paths)
    for value in values:
        if not value:
            continue
        if not any(k in value for k in '/-+@'):
            continue
        if value.startswith('i4x://'):
            value = value.lstrip('i4x://')
        if not value:
            continue
        block = urlparser.urlparse(value).path
        block = block.split('course-v1:')[-1]
        block = block.split('courses/')[-1]
        segments = block.split(':', 1)[-1].split('+')
        segments = '/'.join(map(lambda s: s.split('@')[-1], segments))
        return '/'.join([s for s in segments.split('/') if s][:5])
    return None


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
    segments = course_id.strip().split('/')[:3]
    if len(segments) < 3:
        return os.path.join(
            'UNKNOWN', 'tracklog-{ds}.json{x}'.format(ds=datestr, x=ext)
        )
    return os.path.join(
        '__'.join(segments).replace('.', '_'),
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


def check_for_funny_keys(record, name='toplevel'):
    """
    I am quite frankly not sure what Ike is trying to do here,
    but there should be a better way.
    For now, though, we'll just have to make do.

    :type record: dict
    :param record: Dictionary whose values are modified
    :type name: str
    :param name: Name of the level of the dict
    :rtype: None
    :return: Modifies the record in place
    """
    for key in list(record):
        val = record[key]
        if key.startswith('i4x-') or key.startswith('xblock.'):
            return True
        if key[0] in '0123456789':
            return True
        if '-' in key or '.' in key:
            newkey = key.replace('-', '_').replace('.', '__')
            record[newkey] = val
            record.pop(key)
            key = newkey
        if isinstance(val, dict):
            ret = check_for_funny_keys(val, name + '/' + key)
            if ret:
                record[key] = json.dumps(val)
    return False


def stringify_dict(record, *keys):
    """
    Given a dictionary and some keys, JSON stringify
    the values at those keys in place.

    :type record: dict
    :param record: Dictionary whose values are modified
    :type keys: Iterable[str]
    :param keys: multiple args
    :rtype: None
    :return: Modifies the dict in place
    """
    for key in keys:
        if isinstance(key, (list, tuple)) and len(key) > 1:
            key, subkey, *_ = key
            if key not in record or subkey not in record[key]:
                continue
            record[key][subkey] = json.dumps(record[key][subkey])
        else:
            if key not in record:
                continue
            record[key] = json.dumps(record[key])


def move_unknown_fields_to_agent(record, *keys):
    """
    Move the values associated with the given keys
    into record['agent']

    :type record: dict
    :param record: Dictionary whose values are modified
    :type keys: Iterable[str]
    :param keys: multiple args
    :rtype: None
    :return: Modifies the record in place
    """
    agent = {'oldagent': record.get('agent', '')}
    for key in keys:
        if '.' in key:
            prefix, subkey = key.split('.', 1)
            if prefix in record:
                subrecord = record[prefix]
                if subkey in subrecord:
                    agent[key] = subrecord[subkey]
                    subrecord.pop(subkey)
        else:
            if key in record:
                agent[key] = record[key]
                record.pop(key)
    record['agent'] = json.dumps(agent)


def is_float(val):
    """
    Check that the string can be coerced into a float.
    """
    try:
        float(val)
        return True
    except (TypeError, ValueError):
        return False


def move_field_to_mongoid(record: dict, path: list):
    """
    Move the values associated with the given path
    into record['mongoid']

    :type record: dict
    :param record: Dictionary whose values are modified
    :type keys: Iterable[str]
    :param keys: A list of keys to traverse and move
    :rtype: None
    :return: Modifies the record in place
    """
    mongoid = record.get('mongoid')
    if not isinstance(mongoid, dict):
        mongoid = {'old_mongoid': mongoid}
    key = path[0]
    if len(path) == 1:
        if key in record:
            val = record.pop(key)
            mongoid[key] = val
            return
        return
    if key not in mongoid:
        mongoid[key] = {}
    return move_field_to_mongoid(record, path[1:])


def drop_empties(record, *keys):
    """
    Recursive drop keys whose corresponding values are empty
    from the given record.

    :type record: dict
    :param record: Dictionary whose values are modified
    :type keys: Iterable[str]
    :param keys: multiple args
    :rtype: None
    :return: Modifies the record in place
    """
    if not keys:
        return
    key = keys[0]
    if isinstance(record, dict) and key in record:
        if len(keys) == 1:
            if record[key] == '':
                record.pop(key)
        else:
            drop_empties(record[key], *keys[1:])


def rephrase_record(record: dict):
    """
    Update the given record in place. The purpose of this function
    is to turn this record into something with the same schema as that of
    the target BigQuery table.

    :type record: dict
    :param record: A deserialized JSON record
    :rtype: None
    :return: Nothing, but updates the given record in place
    """
    record['course_id'] = get_course_id(record)
    record['module_id'] = get_module_id(record)
    if 'event' not in record:
        record['event'] = {}
    event = record.get('event')
    try:
        if not isinstance(event, dict):
            event = json.loads(event)
        event_js = True
    except:
        event_js = False
    record['event'] = event
    record['event_js'] = event_js
    event = record.get('event')
    if event is not None:
        record['event'] = json.dumps(event)
    event_type = record.get('event_type', '')
    known_types = set([
        'play_video', 'seq_goto', 'seq_next', 'seq_prev',
        'seek_video', 'load_video', 'save_problem_success',
        'save_problem_fail', 'reset_problem_success',
        'reset_problem_fail', 'show_answer',
        'edx.course.enrollment.activated',
        'edx.course.enrollment.deactivated',
        'edx.course.enrollment.mode_changed',
        'edx.course.enrollment.upgrade.succeeded', 'speed_change_video',
        'problem_check', 'problem_save', 'problem_reset'
    ])
    if isinstance(event, dict):
        outs = ('video_embedded', 'harvardx.button', 'harvardx.')
        out_conds = not any(k in event_type for k in outs)
        in_conds = 'problem_' in event_type or event_type in known_types
        if in_conds and out_conds:
            record['event_struct'] = event
        else:
            record['event_struct'] = {
                'GET': json.dumps(event.get('GET')),
                'POST': json.dumps(event.get('POST')),
                'query': event.get('query')
            }
    else:
        if 'event_struct' in record:
            record.pop('event_struct')
    if '_id' in record:
        record['mongoid'] = record['_id']['$oid']
        record.pop('_id')
    if isinstance(event, dict):
        if 'POST' in event:
            event['POST'] = json.dumps(event['POST'])
        if 'GET' in event:
            event['GET'] = json.dumps(event['GET'])
        if 'child-id' in event:
            event['child_id'] = event['child-id']
            event.pop('child-id')

    problem_events = set([
        'problem_check', 'problem_save', 'problem_reset'
    ])
    if event_type in problem_events and record['event_source'] == 'browser':
        if isinstance(event, (str, list, tuple)):
            event = {'data': json.dumps(event)}
    if isinstance(event, (str, list, tuple)):
        event = {'data': json.dumps(event)}
    to_stringify = (
        ('state', 'input_state'), ('state', 'correct_map'),
        ('state', 'student_answers'),
        'correct_map', 'answers', 'submission', 'old_state',
        'new_state', 'permutation', 'options_selected', 'corrections',
    )
    if event is not None:
        stringify_dict(event, *to_stringify)
    context = record.get('context', {})
    stringify_dict(context, 'course_user_tags')
    mobile_api_context_fields = [
        'application', 'client', 'received_at', 'component',
        'open_in_browser_url', 'module.usage_key',
        'module.original_usage_version', 'module.original_usage_key',
        'asides',
    ]
    move_unknown_fields_to_agent(context, *mobile_api_context_fields)
    mongo_paths = [
        ['referer'], ['accept_language'],
        ['event_struct', 'requested_skip_interval'],
        ['event_struct', 'submitted_answer'],
        ['event_struct', 'num_attempts'], ['event_struct', 'task_id'],
        ['event_struct', 'content'], ['nonInteraction'], ['label'],
        ['event_struct', 'widget_placement'], ['event_struct', 'tab_count'],
        ['event_struct', 'current_tab'], ['event_struct', 'target_tab'],
        ['event_struct', 'state', 'has_saved_answers'], ['context', 'label'],
        ['roles'], ['environment'], ['minion_id'],
        ['event_struct', 'duration'], ['event_struct', 'play_medium']
    ]
    for path in mongo_paths:
        move_field_to_mongoid(record, path)
    drop_empties(record, 'context', 'user_id')
    record.pop('event_js', '')
    if record.get('event_type') == 'speed_change_video':
        speed = record.get('event_struct', {}).get('new_speed')
        if is_float(speed):
            if math.isnan(float(speed)):
                record['event_struct'].pop('new_speed', '')
    check_for_funny_keys(record)
