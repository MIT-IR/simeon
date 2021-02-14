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


CID_PATT1 = re.compile(r'^http[s]*://[^/]+/courses/([^/]+/[^/]+/[^/]+)/', re.I)
CID_PATT2 = re.compile(r'/courses/([^/]+/[^/]+/[^/]+)/', re.I)
# The following are patterns from edx2bigquery that try to pluck out
# the module ID of an event
MI_PATT1 = re.compile(
    r'/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+'
    r'(?P<semester>[^+]+)/xblock/block-v1:[^+]+\+[^+]+\+[^+]+\+type@'
    r'(?P<mtype>[^+]+)\+block@(?P<id>[^/]+)/handler'
)
MI_PATT2 = re.compile(
    r'/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+'
    r'(?P<semester>[^+]+)/[bc]'
)
MI_PATT2A = re.compile(r'input_(?P<id>[^=]+)_[0-9]+_[^=]+=')
MI_PATT3 = re.compile(
    r'/courses/(?P<org>[^/]+)/(?P<course>[^/]+)/(?P<semester>[^+]+)'
    r'/courseware/(?P<chapter>[^/]+)/(?P<sequential>[^/]+)/'
)
MI_PATT3A = re.compile(
    r'input_i4x-(?P<org>[^-]+?)-(?P<course>[^-]+?)-'
    r'(?P<mtype>[^-]+?)-(?P<id>.+?)_[0-9]+_[^=]+=.*'
)
MI_PATT4 = re.compile(
    r'/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+'
    r'(?P<semester>[^+]+)/xblock/block-v1:[^+]+\+[^+]+\+[^+]+\+type@'
    r'(?P<mtype>[^+]+)\+block@(?P<id>[^/]+)'
)
MI_PATT5 = re.compile(
    r'block-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+(?P<semester>[^+]+)\+type@'
    r'(?P<mtype>[^+]+)\+block@(?P<id>[^/]+)'
)
MI_PATT6 = re.compile(
    r'/courses/course-v1:(?P<org>[^+]+)\+(?P<course>[^+]+)\+'
    r'(?P<semester>[^+]+)/courseware/(?P<chapter>[^/]+)/(?P<id>[^/]+)/'
)
MI_PATT6A = re.compile(
    r'i4x-(?P<org>[^-]+)-(?P<course>[^+]+)-(?P<mtype>[^-]+)-(?P<id>[^-]+)'
)
MI_PATT7 = re.compile(r'i4x://([^/]+/[^/]+/[^/]+/[^/]+)')
MI_PATT7A = re.compile(r'i4x://([^/]+/[^/]+/[^/]+/[^/]+)/goto_position')
MI_PATT7B = re.compile(
    r'i4x://(?P<org>[^/]+)/(?P<course>[^/]+)/'
    r'(?P<mtype>[^/]+)/(?P<id>[^/]+)/(?P<action>[^/]+)'
)
MI_PATT7C = re.compile(
    r'i4x://(?P<org>[^/]+)/(?P<course>[^/]+)/'
    r'(?P<mtype>[^/]+)/(?P<id>[^/]+)'
)
MI_PATT8 = re.compile(
    r'/courses/([^/]+/[^/]+)/[^/]+/courseware/[^/]+/([^/]+)/(|[#]*)$'
)
MI_PATT9 = re.compile(
    r'/courses/([^/]+/[^/]+)/[^/]+/courseware/([^/]+)/$'
)
MI_PATT10 = re.compile(
    r'^input_i4x-([^\-]+)-([^\-]+)-problem-([^ =]+)'
    r'_([0-9]+)_([0-9]+)(|_comment|_dynamath)='
)
MI_PATT11 = re.compile(
    r'^/courses/([^/]+/[^/]+)/([^/]+)/discussion/threads/([^/]+)'
)
MI_PATT12 = re.compile(
    r'^/courses/([^/]+/[^/]+)/([^/]+)/discussion/forum/i4x([^/]+)/threads/([^/]+)'
)
MI_PATT13 = re.compile(
    r'^/courses/([^/]+/[^/]+)/([^/]+)/discussion/i4x([^/]+)/threads/create'
)
MI_PATT14 = re.compile(
    r'^/courses/([^/]+/[^/]+)/([^/]+)/discussion/forum/([^/]+)/threads/([^/]+)'
)
MI_PATT15 = re.compile(
    r'/courses/([^/]+/[^/]+)/[^/]+/courseware/[^/]+/([^/]+)/$'
)
MI_PATT16 = re.compile(
    r'/courses/([^/]+/[^/]+)/[^/]+/jump_to_id/([^/]+)(/|)$'
)
MI_PATT17 = re.compile(
    r'/courses/(?P<org>[^/]+)/(?P<course>[^/]+)/'
    r'(?P<semester>[^+]+)/xblock/i4x:;_;_[^/]+;_[^/]+;_'
    r'(?P<mtype>[^;]+);_(?P<id>[^/]+)/handler/.*'
)
MI_PATT18 = re.compile(r'i4x-([^\-]+)-([^\-]+)-video-([^ ]+)')
SQL_FILE_EXTS = {
    '.failed': 3,
    '.gz': 3,
    '.json': 3,
    '.mongo': 1,
    '.sql': 3,
}


def decrypt_files(fnames, verbose=True, logger=None, timeout=60):
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
    :return: Returns True if the decryption fails
    :raises: DecryptionError
    """
    if isinstance(fnames, str):
        fnames = [fnames]
    verbosity = '--verbose' if verbose else ''
    cmd = 'gpg {v} --batch --yes --decrypt-files {f}'.format(
        f=' '.join(fnames), v=verbosity
    )
    if verbose and logger is not None:
        logger.info(cmd[:200])
    proc =  sb.Popen(cmd.split(), stdout=sb.PIPE, stderr=sb.PIPE)
    if proc.wait(timeout=timeout) != 0:
        err = proc.stderr.read().decode('utf8', 'ignore').strip()
        raise DecryptionError(
            'Failed to decrypt {f}: {e}'.format(f=' '.join(fnames), e=err)
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

    :NOTE: Please unit test me!
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
            cid.replace('-', '__', 2).replace('-', '_').replace('.', '_'),
            out,
        )
    )


def get_course_id(record: dict, org_keywords=('mit', 'vj')) -> str:
    """
    Given a JSON record, try getting the course_id out of it.

    :NOTE: Please unit test me!
    :type record: dict
    :param record: A deserialized JSON record
    :type org_keywords: Iterable[str]
    :param org_keywords: Iterable of keywords pertaining to your organization
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


def pluck_match_groups(match, names):
    """
    Pluck the strings matching the given names
    out of the given SRE_Match object.
    Make a string with the matched strings

    :type match: SRE_Match
    :param match: A regex match object from re.search or re.match
    :type names: Union[Tuple, List]
    :param names: A list of group names to pluck out of the match object
    :rtype: str
    :return: A concatenated string from the found matches
    """
    mdict = match.groupdict()
    if not mdict:
        mdict = dict(enumerate(match.groups(), 1))
    return '/'.join(mdict.get(n, n) for n in names if n in mdict)


def get_module_id(record: dict, org_keywords=('mit', 'vj')):
    """
    Get the module ID of the given record

    :type record: dict
    :param record: A deserialized JSON record
    :type org_keywords: Iterable[str]
    :param org_keywords: Iterable of keywords pertaining to your organization
    :rtype: str
    :return: A valid edX course ID or an empty string
    """
    event = record['event']
    event_type = record['event_type']
    path = record.get('context', {}).get('path', '')
    for chunk in (event_type, path):
        match = MI_PATT1.search(chunk)
        if match:
            return pluck_match_groups(
                match, ['org', 'course', 'semester', 'mtype', 'id']
            )
    conds = [
        all([
            'problem' in event_type,
            isinstance(event, str) and event.startswith('input_'),
        ]),
        all([
            event_type == 'problem_graded',
            bool(event),
            isinstance(event, list) and event[0].startswith('input_')
        ])
    ]
    page = record.get('page', '') or ''
    for cond in conds:
        if cond:
            match = MI_PATT2.search(page)
            if match:
                if isinstance(event, list):
                    substr = event[0]
                else:
                    substr = event.split('&', 1)[0]
                submatch = MI_PATT2A.search(substr)
                if submatch:
                    return pluck_match_groups(
                        match, ['org', 'course', 'semester', 'problem', 'id']
                    )
            if isinstance(event, list):
                match = MI_PATT3A.search(event[0])
            else:
                match = MI_PATT3A.search(event)
            if match:
                return pluck_match_groups(
                    match, ['org', 'course', 'semester', 'mtype', 'id']
                )
    for chunk in (event_type, path):
        match = MI_PATT4.search(chunk)
        if match:
            return pluck_match_groups(
                match, ['org', 'course', 'semester', 'mtype', 'id']
            )
    if not isinstance(event, dict):
        try:
            event_dict = json.loads(event)
        except:
            event_dict = None
        if isinstance(event_dict, dict) and 'id' in event_dict:
            event = event_dict
    if isinstance(event, dict) and isinstance(event.get('id'), str):
        event_id = event['id']
        match = MI_PATT5.search(event_id)
        if match:
            return pluck_match_groups(
                match, ['org', 'course', 'semester', 'mtype', 'id']
            )
        match = MI_PATT6A.search(event_id)
        if match:
            return pluck_match_groups(
                match, ['org', 'course', 'semester', 'mtype', 'id']
            )
        if event_type == 'play_video' and '/' not in event_id:
            match = MI_PATT6.search(page)
            if match:
                return pluck_match_groups(
                    match, ['org', 'course', 'semester', 'video', event_id]
                )
    elif isinstance(event, str):
        match = MI_PATT5.search(event)
        if match:
            return pluck_match_groups(
                match, ['org', 'course', 'semester', 'mtype', 'id']
            )
    bad_events = ('add_resource', 'delete_resource', 'recommender_upvote')
    if event_type in bad_events:
        return None
    if isinstance(event, dict):
        if 'id' in event and not isinstance(event.get('id'), str):
            return None
    if record.get('event_source') == 'browser':
        try:
            match = MI_PATT7.search(event.get('id', ''))
            if match:
                if event_type == 'seq_goto' or event_type == 'seq_next':
                    return match.group(1) + '/' + event.get('new', '')
                return match.group(1)
        except:
            pass
        if event_type == 'page_close':
            match = MI_PATT8.search(page)
            if match:
                return match.group(1) + '/sequential/' + match.group(2) + '/'
            match = MI_PATT9.search(page)
            if match:
                return match.group(1) + '/chapter/' + match.group(2) + '/'
        try:
            match = MI_PATT7.search(event.get('problem', ''))
            if match:
                return match.group(1)
        except:
            pass
        if isinstance(event, str):
            substr = event
        elif isinstance(event, (list, tuple)):
            substr = event[0]
        else:
            substr = ''
        try:
            match = MI_PATT10.search(substr)
            if match:
                return '/'.join((
                    match.group(1), match.group(2),
                    'problem', match.group(3)
                ))
        except:
            pass
    patt_names = (
        (MI_PATT11, (1, 'forum', 3)),
        (MI_PATT12, (1, 'forum', 4)),
        (MI_PATT13, (1, 'forum', 'new')),
        (MI_PATT14, (1, 'forum', 4)),
        (MI_PATT15, (1, 'sequential', 2, '')),
        (MI_PATT9, (1, 'chapter', 2)),
        (MI_PATT16, (1, 'jump_to_id', 2)),
        (MI_PATT17, ('org', 'course', 'semester', 'mtype', 'id')),
    )
    for patt, names in patt_names:
        match = patt.search(event_type)
        if match:
            return pluck_match_groups(match, names)
    match = MI_PATT17.search(path)
    if match:
        return pluck_match_groups(
            match, ['org', 'course', 'semester', 'mtype', 'id']
        )
    if isinstance(event, str) and event.startswith('input_'):
        match = MI_PATT3A.search(event)
        if match:
            return pluck_match_groups(
            match, ['org', 'course', 'semester', 'mtype', 'id']
        )
    match = MI_PATT7.search(event_type)
    if match:
        if MI_PATT7A.search(event_type):
            try:
                return match.group(1) + '/' + event['POST']['position'][0]
            except:
                pass
        return match.group(1)
    match = MI_PATT7B.search(event_type)
    if match:
        return pluck_match_groups(match, ['org', 'course', 'semester', 'mtype', 'id'])
    if isinstance(event, str):
        match = MI_PATT7C.search(event)
        if match:
            return pluck_match_groups(match, ['org', 'course', 'semester', 'mtype', 'id'])
    if isinstance(event, dict):
        keys = [
            ('problem_id', MI_PATT7, (1,)),
            ('id', MI_PATT18, (1, 2, 'video', 3)),
        ]
        for key, patt, names in keys:
            if event.get(key):
                match = patt.search(event[key])
                if match:
                    return pluck_match_groups(match, names)
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
    for key, val in record.items():
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


def rephrase_record(record: dict, org_keywords=('mit', 'vj')):
    """
    Update the given record in place. The purpose of this function
    is to turn this record into something with the same schema as that of
    the target BigQuery table.

    :type record: dict
    :param record: A deserialized JSON record
    :rtype: None
    :return: Nothing
    """
    if 'course_id' not in record:
        record['course_id'] = get_course_id(record, org_keywords)
    if 'module_id' not in record:
        record['module_id'] = get_module_id(record, org_keywords)
    if 'event' not in record:
        record['event'] = ''
    if 'event_js' not in record:
        event = record.get('event')
        try:
            if not isinstance(event, dict):
                event = json.loads(event)
            event_js = True
        except:
            event_js = False
        record['event'] = event
        record['event_js'] = event_js
    event = None
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
