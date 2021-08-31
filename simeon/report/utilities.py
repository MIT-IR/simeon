"""
Utility functions and classes to help with making course reports like user_info_combo, person_course, etc.
"""
import csv
import glob
import gzip
import json
import math
import multiprocessing as mp
import os
import re
import signal
import sys
import tarfile
import traceback
from collections import OrderedDict, defaultdict
from datetime import datetime
from functools import reduce
from multiprocessing.pool import (
    Pool as ProcessPool, ThreadPool, TimeoutError
)
from xml.etree import ElementTree

from dateutil.parser import parse as parse_date
from jinja2 import Template
from google.cloud.exceptions import NotFound

from simeon.download import utilities as downutils
from simeon.exceptions import (
    BadSQLFileException, EarlyExitError, LoadJobException,
    MissingFileException, MissingQueryFileException, MissingSchemaException,
    SchemaMismatchException, SQLQueryException,
)
from simeon.upload import gcp
from simeon.upload import utilities as uputils


# Increase the csv module's field size limit
csv.field_size_limit(13107200)

# BigQuery client for processes in process pools
report_bq_client = None


# Set up schema coercion functions
def _format_str_date(d):
    # return parse_date(d).strftime('%Y-%m-%d %H:%M:%S.%f')
    return parse_date(d).isoformat()


def _to_float(v):
    v = float(v)
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _stringify(v):
    if v is None:
        return None
    if not isinstance(v, str):
        v = json.dumps(v)
    return v.strip()


def _delete_incomplete_matches(dirname, patt):
    matches = glob.iglob(
        os.path.join(dirname, '*{p}*.json.gz'.format(p=patt))
    )
    for file_ in matches:
        try:
            os.remove(file_)
        except:
            continue


BQ2PY_TYPES = {
    'TIMESTAMP': _format_str_date,
    'STRING': _stringify,
    'INTEGER': int,
    'FLOAT': _to_float,
    'BOOLEAN': bool,
}


BQ_DDL = """#standardSQL
CREATE OR REPLACE TABLE {table} {cols}
OPTIONS (
    description = '''{description}'''
) AS
{query}"""
USER_INFO_COLS = OrderedDict([
    (
        ('auth_user-analytics.sql', None),
        [
            'user_id', 'username', 'email', 'is_staff',
            'last_login', 'date_joined',
        ],
    ),
    (
        ('auth_userprofile-analytics.sql', 'profile'),
        [
            'profile_name', 'profile_language', 'profile_location',
            'profile_meta', 'profile_courseware', 'profile_gender',
            'profile_mailing_address', 'profile_year_of_birth',
            'profile_level_of_education', 'profile_goals',
            'profile_allow_certificate', 'profile_country', 'profile_city',
        ],
    ),
    (
        ('student_courseenrollment-analytics.sql', 'enrollment'),
        [
            'enrollment_course_id', 'enrollment_created',
            'enrollment_is_active', 'enrollment_mode'
        ],
    ),
    (
        ('certificates_generatedcertificate-analytics.sql', 'certificate'),
        [
            'certificate_id', 'certificate_user_id',
            'certificate_download_url', 'certificate_grade',
            'certificate_course_id', 'certificate_key',
            'certificate_distinction', 'certificate_status',
            'certificate_verify_uuid', 'certificate_download_uuid',
            'certificate_name', 'certificate_created_date',
            'certificate_modified_date', 'certificate_error_reason',
            'certificate_mode'
        ],
    ),
    (
        ('user_id_map-analytics.sql', 'id_map'),
        ['id_map_hash_id']
    )
])
ADDED_COLS = [
    'edxinstructordash_Grade', 'edxinstructordash_Grade_timestamp',
    'y1_anomalous'
]
SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'upload', 'schemas'
)
QUERY_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'queries',
)

PROBLEM_TYPES = {
    'choiceresponse', 'coderespons', 'customresponse',
    'fieldset', 'formularesponse', 'imageresponse',
    'multiplechoiceresponse', 'numericalresponse',
    'optionresponse', 'stringresponse', 'schematicresponse',
}


def _sql_pool_init():
    """
    Process pool initializer
    """
    def sighandler(sig, frame):
        raise EarlyExitError(
            'SQL table file generation interrupted prematurely'
        )
    sigs = [signal.SIGABRT, signal.SIGTERM, signal.SIGINT]
    for sig in sigs:
        signal.signal(sig, signal.SIG_DFL)


def _report_pool_init(proj, safile=None):
    """
    Process pool initializer
    """
    global report_bq_client
    def sighandler(sig, frame):
        raise EarlyExitError(
            'Secondary table generation interrupted prematurely'
        )
    sigs = [signal.SIGABRT, signal.SIGTERM, signal.SIGINT]
    for sig in sigs:
        signal.signal(sig, signal.SIG_DFL)
    if not safile:
        report_bq_client = gcp.BigqueryClient(project=proj)
    else:
        report_bq_client = gcp.BigqueryClient.from_service_account_json(
            safile, project=proj
        )


def wait_for_bq_jobs(job_list):
    """
    Given a list of BigQuery load or query jobs,
    wait for them all to finish.

    :type job_list: Iterable[LoadJob]
    :param job_list: An Iterable of job objects from the bigquery package
    :rtype: None
    :return: Nothing
    :TODO: Improve this function to behave a little less like a tight loop
    """
    done = set()
    while len(done) < len(job_list):
        for job in job_list:
            try:
                if job.job_id not in done:
                    state = job.done()
                    if state:
                        done.add(job.job_id)
            except NotFound:
                msg = '{id} is not a valid BigQuery job ID'
                raise LoadJobException(msg.format(id=job.job_id)) from None


def wait_for_bq_job_ids(job_list, client):
    """
    Given a list of BigQuery load or query job IDs,
    wait for them all to finish.

    :type job_list: Iterable[str]
    :param job_list: An Iterable of job IDs
    :rtype: Dict[str, Dict[str, str]]
    :return: Returns a dict of job IDs to job errors
    :TODO: Improve this function to behave a little less like a tight loop
    """
    out = dict()
    while len(out) < len(job_list):
        for job in job_list:
            if job not in out:
                try:
                    rjob = client.get_job(job)
                    if rjob.state == 'DONE':
                        src = ', '.join(getattr(rjob, 'source_uris', []))
                        err = (rjob.errors or [])
                        for e in err:
                            if e:
                                e['source'] = src
                        out[job] = err
                except NotFound:
                    msg = '{id} is not a valid BigQuery job ID'.format(id=job)
                    raise LoadJobException(msg) from None
    return out


def check_record_schema(record, schema, coerce=True, nullify=False):
    """
    Check that the given record matches the same keys found in the given
    schema list of fields. The latter is one of the schemas in
    simeon/upload/schemas/

    :type record: dict
    :param record: Dictionary whose values are modified
    :type schema: Iterable[Dict[str, Union[str, Dict]]]
    :param schema: A list of dicts with info on BigQuery table fields
    :type coerce: bool
    :param coerce: Whether or not to coerce values into BigQuery types
    :type nullify: bool
    :param nullify: Whether to set values mapping missing keys to None
    :rtype: None
    :returns: Modifies the record if needed
    :raises: SchemaMismatchException
    """
    for field in schema:
        if field.get('field_type') != 'RECORD':
            if field.get('name') not in record and nullify:
                if not coerce:
                    raise SchemaMismatchException(
                        '{f} is missing from the record'.format(
                            f=field.get('name')
                        )
                    )
                record[field.get('name')] = None
            elif field.get('name') in record and coerce:
                val = record[field.get('name')]
                func = BQ2PY_TYPES.get(field.get('field_type'))
                if func and val is not None:
                    try:
                        record[field.get('name')] = func(val)
                    except (ValueError, TypeError):
                        record[field.get('name')] = None
        else:
            subfields = field.get('fields')
            subrecord = record.get(field.get('name'), {})
            check_record_schema(subrecord, subfields, coerce)


def drop_extra_keys(record, schema):
    """
    Walk through the record and drop key-value pairs that are not in the
    given schema

    :type record: dict
    :param record: Dictionary whose values are modified
    :type schema: Iterable[Dict[str, Union[str, Dict]]]
    :param schema: A list of dicts with info on BigQuery table fields
    :rtype: None
    :return: Modifies the record if needed
    """
    if not schema:
        return
    keys = list(record)
    for k in keys:
        if k not in (f.get('name') for f in schema):
            del record[k]
        elif isinstance(record, dict) and isinstance(record.get(k), dict):
            subrecord = record.get(k, {})
            target = next((f for f in schema if f.get('name') == k), None)
            if target is None:
                subfields = []
            else:
                subfields = target.get('fields')
            drop_extra_keys(subrecord, subfields)


def extract_table_query(table, query_dir):
    """
    Given a table name and a query directory,
    extract both the query string and the table description.
    The latter is assumed to be any line in the query file
    that starts with # or --

    :type table: str
    :param table: BigQuery table name whose query info is extracted
    :type query_dir: str
    :param query_dir: The directory where the query file is expected to be
    :rtype: Tuple[str, str]
    :returns: A tuple of strings (query string, table description)
    :raises: MissingQueryFileException
    """
    table = table.split('.')[-1]
    qfile = os.path.join(query_dir, '{t}.sql'.format(t=table))
    if not os.path.exists(qfile):
        msg = 'Table {t} does not have a query file in {d}'
        raise MissingQueryFileException(msg.format(t=table, d=query_dir))
    with open(qfile) as qf:
        description = []
        query = []
        for line in qf:
            if line.startswith('--') or line.startswith('#'):
                description.append(line.lstrip('-# '))
            query.append(line)
    return (''.join(query), ''.join(description))


def make_user_info_combo(
    dirname, schema_dir=SCHEMA_DIR,
    outname='user_info_combo.json.gz'
):
    """
    Given a course's SQL directory, make a user_info_combo report

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the outname argument
    """
    schema_dir = schema_dir or SCHEMA_DIR
    for (file_, _) in USER_INFO_COLS:
        file_ = os.path.join(dirname, file_)
        if not os.path.exists(file_):
            raise OSError(
                '{f} does not exist in the SQL bundle.'.format(f=file_)
            )
    schema_file = os.path.join(
        schema_dir, 'schema_user_info_combo.json'
    )
    if not os.path.exists(schema_file):
        raise MissingSchemaException(
            'The schema file {f} does not exist. '
            'Please provide a valid schema directory.'.format(f=schema_file)
        )
    with open(schema_file) as sfh:
        schema = json.load(sfh).get('user_info_combo')
    users = dict()
    user_file = 'auth_user-analytics.sql'
    user_cols = USER_INFO_COLS.get((user_file, None))
    with open(os.path.join(dirname, user_file)) as ufh:
        incols = [c.strip() for c in ufh.readline().split('\t')]
        reader = csv.DictReader(
            ufh, delimiter='\t', lineterminator='\n',
            quotechar='"', fieldnames=incols
        )
        for row in reader:
            uid = row.get('id')
            row['user_id'] = uid
            users[uid] = dict((k, row.get(k)) for k in user_cols)
    for (fname, prefix), cols in USER_INFO_COLS.items():
        if fname == user_file:
            continue
        with open(os.path.join(dirname, fname)) as rfh:
            header = []
            for col in map(str.strip, rfh.readline().split('\t')):
                if prefix:
                    uid_col = '{p}_user_id'.format(p=prefix)
                    username_col = '{p}_username'.format(p=prefix)
                    header.append('{p}_{c}'.format(p=prefix, c=col))
                else:
                    uid_col = 'user_id'
                    username_col = 'username'
                    header.append(col)
            reader = (dict(zip(header, r.split('\t'))) for r in rfh)
            # reader = csv.DictReader(
            #     (l.replace('"read more', 'read more') for l in rfh),
            #     delimiter='\t', lineterminator='\n',
            #     # quotechar='\'',
            #     fieldnames=header
            # )
            for row in reader:
                if uid_col not in row:
                    row[uid_col] = row.get('{p}_id'.format(p=prefix))
                user_id = row.get(uid_col)
                target = users.setdefault(user_id, {})
                target['user_id'] = user_id
                target.update(dict((k, row.get(k)) for k in cols))
                if target.get('username') is None and row.get(username_col):
                    target['username'] = row[username_col]
    outcols = reduce(lambda l, r: l + r, USER_INFO_COLS.values())
    outcols += ADDED_COLS
    with gzip.open(os.path.join(dirname, outname), 'wt') as zh:
        for record in users.values():
            outrow = dict()
            for k in outcols:
                val = record.get(k)
                val = val.strip() if val else val
                if 'course_id' in k:
                    val = downutils.get_sql_course_id(val or '') or None
                if 'certificate_grade' in k:
                    try:
                        val = str(float(val))
                    except (TypeError, ValueError):
                        val = None
                if val == 'NULL' or val == 'null':
                    outrow[k] = None
                else:
                    outrow[k] = val
            id_cols = ('user_id', 'certificate_user_id')
            if all(not outrow.get(k) for k in id_cols):
                continue
            # check_record_schema(outrow, schema, True)
            drop_extra_keys(outcols, schema)
            check_record_schema(outrow, schema)
            zh.write(json.dumps(outrow) + '\n')
        zh.flush()


def course_from_block(block):
    """
    Extract a course ID from the given block ID

    :type block: str
    :param block: A module item's block string
    :rtype: str
    :returns: Extracts the course ID in a module's block string
    """
    if block.startswith('i4x://'):
        return block.split('//')[-1].replace('course/', '')
    return '/'.join(block.split(':')[-1].split('+', 3)[:3])


def module_from_block(block):
    """
    Extract a module ID from the given block

    :type block: str
    :param block: A module item's block string
    :rtype: str
    :returns: Extracts the module ID in a module's block string
    """
    block = block.replace('/courses/course-v1:', '')
    if block.startswith('i4x://'):
        return block.lstrip('i4x://')
    segments = block.split(':', 1)[-1].split('+')
    segments = '/'.join(map(lambda s: s.split('@')[-1], segments))
    return '/'.join(segments.split('/')[:5])


def get_youtube_id(record):
    """
    Given a course structure record, extract the YouTube ID
    associated with the video element.

    :type record: dict
    :param record: A course_axis record
    :rtype: Union[str, None]
    :returns: The YouTube video ID associated with the record
    """
    for k, v in record.get('metadata', {}).items():
        if 'youtube_id' in k and v:
            return ':'.join(re.findall(r'\d+', k) + [v])


def _get_itypes(fname, problem_types=PROBLEM_TYPES):
    """
    Extract values for the course_axis.data.itype field
    from the given tar file.
    """
    out = dict()
    with tarfile.open(fname) as tf:
        for problem in tf.getmembers():
            if '/problem/' not in problem.name or problem.isdir():
                continue
            block = os.path.splitext(problem.name)[0].split('/')[-1]
            pf = tf.extractfile(problem)
            root = ElementTree.fromstring(pf.read())
            for elm in root:
                if elm.tag in problem_types:
                    out[block] = elm.tag
                    break
    return out


def get_has_solution(record):
    """
    Extract whether the given record is a problem that has showanswer.
    If it's present and its associated value is not "never", then return True.
    Otherwise, return False.

    :type record: dict
    :param record: A course_axis record
    :rtype: bool
    :returns: Wether the course_axis record has a solution in the data
    """
    meta = record.get('metadata') or dict()
    if 'showanswer' not in meta:
        return False
    return meta['showanswer'] != 'never'


def get_problem_nitems(record):
    """
    Get a value for data.num_items in course_axis

    :type record: dict
    :param record: A course_axis record
    :rtype: Union[int, None]
    :returns: The number of subitems of a problem item
    """
    if 'problem' in record.get('category', ''):
        return len(record.get('children', [])) + 1
    return None


def _get_axis_path(block, mapping):
    """
    Extract the path to the given block from the root
    of the course structure.

    :type block: str
    :param block: One of the block IDs or names from the course structure file
    :type mapping: dict
    :param mapping: A dict mapping child blocks to their parents
    :rtype: str
    :return: A constructed path from root to the given block with hashes
    """
    if '/course/' in block:
        return '/'
    path = []
    while block:
        path.append(module_from_block(block))
        block = mapping.get(block)
    return '/{p}'.format(p='/'.join(
        map(lambda s: s.split('/')[-1], path[-2::-1])
    ))


def _get_first_axis_meta(block, name, struct, mapping):
    """
    Get the first non-null or non-empty value
    of the given metadata name from the data dictionary
    starting at the given block.
    Use the mapping dictionary to find the parents of items
    to consider
    """
    struct = (struct or dict())
    out = None
    while block:
        out = struct.get(block, {}).get('metadata', {}).get(name)
        if out:
            break
        block = mapping.get(block)
    return out


def process_course_structure(data, start, mapping, parent=None):
    """
    The course structure data dictionary and starting point,
    loop through it and construct course axis data items

    :type data: dict
    :param data: The data from the course_structure-analytics.json file
    :type start: str
    :param start: The key from data to start looking up children
    :type mapping: dict
    :param mapping: A dict mapping child blocks to their parents
    :type parent: Union[None, str]
    :param parent: Parent of start
    :rtype: List[Dict]
    :return: Returns the list of constructed data items
    """
    out = []
    record = data.get(start, {})
    sep = '/' if start.startswith('i4x:') else '@'
    children = record.get('children', [])
    item = dict(
        parent=parent.split(sep)[-1] if parent else None,
        split_url_name=None,
    )
    item['path'] = _get_axis_path((start or '/course/'), mapping)
    item['category'] = record.get('category', '')
    item['url_name'] = start.split(sep)[-1]
    targets = (
        ('name', 'display_name'), ('gformat', 'format'),
        ('due', 'due'), ('start', 'start'), ('graded', 'graded'),
        ('visible_to_staff_only', 'visible_to_staff_only'),
    )
    for (key, target) in targets:
        item[key] = _get_first_axis_meta(
            block=start, name=target,
            struct=data, mapping=mapping
        )
    item['graded'] = bool(item.get('graded'))
    item['is_split'] = any([
        'split_test' in item['category'],
        'split_test' in data.get(parent, {}).get('category', '')
    ])
    if item['is_split']:
        if 'split_test' in start:
            item['split_url_name'] = item['url_name']
        else:
            item['split_url_name'] = item['parent']
    item['module_id'] = module_from_block(start)
    item['data'] = dict(
        ytid=get_youtube_id(record),
        weight=record.get('metadata', {}).get('weight'),
        group_id_to_child=None,
        user_partition_id=None,
        itype=None,
        num_items=get_problem_nitems(record),
        has_solution=get_has_solution(record),
        has_image=False,
    )
    out.append(item)
    if children:
        for child in children:
            out.extend(
                process_course_structure(
                    data=data, start=child,
                    parent=start, mapping=mapping,
                )
            )
    return out


def make_course_axis(
    dirname, schema_dir=SCHEMA_DIR, outname='course_axis.json.gz'
):
    """
    Given a course's SQL directory, make a course_axis report

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the outname argument
    """
    schema_dir = schema_dir or SCHEMA_DIR
    fname = os.path.join(dirname, 'course_structure-analytics.json')
    bundle = os.path.join(dirname, 'course-analytics.xml.tar.gz')
    for file_ in (fname, bundle):
        if not os.path.exists(file_):
            raise OSError(
                '{f} does not exist in the SQL bundle.'.format(f=file_)
            )
    itypes = _get_itypes(bundle)
    with open(fname) as fh:
        structure: dict = json.load(fh)
    # Find the course object (i.e. root object)
    root_block = None
    root_val = None
    for block, val in structure.items():
        if val.get('category') == 'course':
            root_block = block
            root_val = val
            break
    if not root_block:
        msg = (
            'The given course structure file {f!r} does not have a root'
            ' course block. Please reach out to edX to have them fix it.'
        )
        raise BadSQLFileException(msg.format(f=fname))
    course_id = course_from_block(root_block)
    # Map child items to their parents
    child_to_parent = dict()
    for k, v in structure.items():
        children = v.get('children') or []
        for child in children:
            child_to_parent[child] = k
    data = process_course_structure(
        data=structure, start=root_block,
        mapping=child_to_parent,
    )
    outname = os.path.join(dirname, 'course_axis.json.gz')
    with gzip.open(outname, 'wt') as zh:
        chapter_mid = None
        for index, record in enumerate(data, 1):
            if record.get('category') == 'chapter':
                chapter_mid = record.get('module_id')
            record['course_id'] = course_id
            record['chapter_mid'] = chapter_mid
            record['index'] = index
            record['data']['itype'] = itypes.get(
                record.get('module_id', '').split('/')[-1]
            )
            if record['gformat']:
                if not record.get('due'):
                    record['due'] = root_val.get('end')
                if not record.get('start'):
                    record['start'] = root_val.get('start')
            zh.write(json.dumps(record) + '\n')
        zh.flush()


def make_grades_persistent(
    dirname,
    schema_dir=SCHEMA_DIR,
    first_outname='grades_persistent.json.gz',
    second_outname='grades_persistent_subsection.json.gz'
):
    """
    Given a course's SQL directory, make the grades_persistent
    and grades_persistent_subsection reports.

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the target files
    """
    schema_dir = schema_dir or SCHEMA_DIR
    infiles = dict([
        (
            'grades_persistentcoursegrade-analytics.sql',
            (first_outname, 'schema_grades_persistent.json'),
        ),
        (
            'grades_persistentsubsectiongrade-analytics.sql',
            (second_outname, 'schema_grades_persistent_subsection.json'),
        )
    ])
    for file_, (outname, schema_file) in infiles.items():
        file_ = os.path.join(dirname, file_)
        outname = os.path.join(dirname, outname)
        if not os.path.exists(file_):
            raise OSError(
                '{f} does not exist in the SQL bundle.'.format(f=file_)
            )
        fschema_file = os.path.join(schema_dir, schema_file)
        if not os.path.exists(fschema_file):
            raise MissingSchemaException(
                'The given schema file {f} does not exist. '
                'Please provide a valid schema directory.'.format(f=fschema_file)
            )
        with open(fschema_file) as sfh:
            sname, _ = os.path.splitext(schema_file)
            sname = sname.replace('schema_', '')
            schema = json.load(sfh).get(sname)
        with open(file_) as gh, gzip.open(outname, 'wt') as zh:
            header = [c.strip() for c in gh.readline().split('\t')]
            reader = csv.DictReader(
                gh, delimiter='\t', quotechar='\'',
                lineterminator='\n', fieldnames=header
            )
            for record in reader:
                for k in record:
                    if 'course_id' in k:
                        record[k] = downutils.get_sql_course_id(record[k])
                    if record[k] == 'NULL':
                        record[k] = None
                check_record_schema(record, schema)
                zh.write(json.dumps(record) + '\n')
            zh.flush()


def make_grading_policy(
    dirname,
    schema_dir=SCHEMA_DIR,
    outname='grading_policy.json.gz'
):
    """
    Generate a file to be loaded into the grading_policy table
    of the given SQL directory.

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type schema_dir: Union[None, str]
    :param schema_dir: Directory where schema files live
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the target file
    """
    schema_dir = schema_dir or SCHEMA_DIR
    file_ = os.path.join(dirname, 'course-analytics.xml.tar.gz')
    if not os.path.exists(file_):
        raise OSError(
            '{f} does not exist in the SQL bundle'.format(f=file_)
        )
    with tarfile.open(file_) as tar:
        policy = next(
            (m for m in tar.getmembers() if 'grading_policy.json' in m.name),
            None
        )
        if policy is None:
            raise MissingFileException(
                'No grading policy found in {f!r}'.format(f=file_)
            )
        with tar.extractfile(policy) as jh:
            grading_policy = json.load(jh)
        outname = os.path.join(dirname, outname)
        cols = (
            'assignment_type', 'name', 'fraction_of_overall_grade',
            'min_count', 'drop_count', 'short_label',
            'overall_lower_cutoff', 'overall_lower_cutoff_label',
            'overall_upper_cutoff', 'overall_upper_cutoff_label',
        )
        with gzip.open(outname, 'wt') as zh:
            for grader in grading_policy.get('GRADER', []):
                grader['assignment_type'] = grader.get('type')
                grader['name'] = grader.get('type')
                grader['fraction_of_overall_grade'] = grader.get('weight')
                cutoffs = sorted(
                    (grading_policy.get('GRADE_CUTOFFS') or {}).items(),
                    key=lambda t: t[1]
                )
                if cutoffs:
                    grader['overall_lower_cutoff'] = cutoffs[0][1]
                    grader['overall_lower_cutoff_label'] = cutoffs[0][0]
                    grader['overall_upper_cutoff'] = cutoffs[-1][1]
                    grader['overall_upper_cutoff_label'] = cutoffs[-1][0]
                else:
                    grader['overall_lower_cutoff'] = None
                    grader['overall_lower_cutoff_label'] = None
                    grader['overall_upper_cutoff'] = None
                    grader['overall_upper_cutoff_label'] = None
                zh.write(
                    json.dumps(dict((k, grader.get(k)) for k in cols)) + '\n'
                )
            zh.flush()


def _extract_mongo_values(record, key, subkey):
    """
    Given a forum data dictionary with immediate and sub keys,
    extract the value(s) at subkey and jsonify them, if need be.
    """
    target = record.get(key)
    if not target:
        return target
    if isinstance(target, dict):
        return target.get(subkey)
    if isinstance(target, list):
        out = []
        for subrec in target:
            if subkey in subrec:
                out.append(subrec[subkey])
        return json.dumps(out)
    return None


def make_forum_table(
    dirname,
    schema_dir=SCHEMA_DIR,
    outname='forum.json.gz'
):
    """
    Generate a file to load into the forum table
    using the given SQL directory

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type schema_dir: str
    :param schema_dir: Directory where schema files live
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the target file
    """
    schema_dir = schema_dir or SCHEMA_DIR
    outname = os.path.join(dirname, outname)
    file_ = os.path.join(dirname, 'forum.mongo')
    if not os.path.exists(file_):
        raise OSError(
            '{f} does not exist in the SQL bundle.'.format(f=file_)
        )
    cols = {
        '$oid': (
            '_id', 'parent_id', 'parent_ids', 'comment_thread_id',
        ),
        '$date': (
            ('endorsement', 'time'), 'updated_at',
            'created_at', 'last_activity_at'
        ),
        None: (
            '_type', 'abuse_flaggers', 'anonymous', 'anonymous_to_peers',
            'at_position_list', 'author_id', 'author_username', 'body',
            'child_count', 'closed', 'comment_count', 'commentable_id',
            'context', 'course_id', 'depth', 'endorsed',
            'historical_abuse_flaggers', 'pinned', 'retired_username',
            'sk', 'thread_type', 'title', 'visible', 'votes'
        ),
    }
    schema_file = os.path.join(
        schema_dir, 'schema_forum.json'
    )
    if not os.path.exists(schema_file):
        raise MissingSchemaException(
            'The schema file {f} does not exist. '
            'Please provide a valid schema directory.'.format(f=schema_file)
        )
    with open(schema_file) as sfh:
        schema = json.load(sfh).get('forum')
    with open(file_) as fh, gzip.open(outname, 'wt') as zh:
        for line in fh:
            record = json.loads(line)
            for subkey, keys in cols.items():
                for col in keys:
                    if isinstance(col, (tuple, list)):
                        col, subcol = col[:2]
                    else:
                        subcol = None
                    if subkey is not None:
                        if subcol:
                            val = _extract_mongo_values(
                                (record.get(col, {}) or {}), subcol, subkey
                            )
                            if not isinstance(record.get(col), dict):
                                record[col] = {}
                            record[col][subcol] = val
                        else:
                            record[col] = _extract_mongo_values(
                                record, col, subkey
                            )
                    if record.get(col, '') == 'NULL':
                        record[col] = None
                    if isinstance(record.get(col), list):
                        record[col] = json.dumps(record[col])
                    if isinstance(record.get(col), dict):
                        for k in record[col]:
                            if isinstance(record[col][k], list):
                                record[col][k] = json.dumps(record[col][k])
            record['mongoid'] = record['_id']
            record['course_id'] = downutils.get_sql_course_id(
                record.get('course_id') or ''
            )
            drop_extra_keys(record, schema)
            check_record_schema(record, schema, True)
            zh.write(json.dumps(record) + '\n')
        zh.flush()


def make_problem_analysis(state, **extras):
    """
    Use the state record from a studentmodule record to make
    a record to load into the problem_analysis table.
    The state is assumed to be from record of category "problem".

    :type state: dict
    :param state: Contents of the state field of studentmodule
    :type extras: keyword arguments
    :param extras: Things to be added to the generated record
    :rtype: dict
    :return: Return a record to be loaded in problem_analysis
    """
    maps = state.get('correct_map') or {}
    answers = state.get('student_answers') or {}
    items = []
    for k, v in maps.items():
        items.append(
            {
                'answer_id': k,
                'correctness': v.get('correctness'),
                'correct_bool' : v.get('correctness', '') == 'correct',
                'npoints': v.get('npoints'),
                'msg': v.get('msg'),
                'hint': v.get('hint'),
                'response': json.dumps(answers.get(k)),
            }
        )
    out = {
        'item': items,
        'attempts': state.get('attempts', 0),
        'done': state.get('done'),
    }
    out.update(extras)
    return out


def make_student_module(
    dirname,
    schema_dir=SCHEMA_DIR,
    outname='studentmodule.json.gz'
):
    """
    Generate files to load into studentmodule and problem_analysis
    using the given SQL directory

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type schema_dir: str
    :param schema_dir: Directory where schema files live
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the target files
    """
    schema_dir = schema_dir or SCHEMA_DIR
    pjoin = os.path.join
    outname = pjoin(dirname, outname)
    second = pjoin(dirname, 'problem_analysis.json.gz')
    file_ = pjoin(dirname, 'courseware_studentmodule-analytics.sql')
    if not os.path.exists(file_):
        raise OSError(
            '{f} does not exist in the SQL bundle.'.format(f=file_)
        )
    with open(pjoin(schema_dir, 'schema_studentmodule.json')) as sfh:
        module_schema = json.load(sfh).get('studentmodule')
    with open(pjoin(schema_dir, 'schema_problem_analysis.json')) as sfh:
        problem_schema = json.load(sfh).get('problem_analysis')
    prob_cols = ('correct_map', 'student_answers')
    with open(file_, encoding='UTF8', errors='ignore') as fh:
        header = [c.strip() for c in fh.readline().split('\t')]
        reader = csv.DictReader(
            (l.replace('\0', '') for l in fh),
            delimiter='\t', quotechar='\'', lineterminator='\n',
            fieldnames=header
        )
        with gzip.open(outname, 'wt') as zh, gzip.open(second, 'wt') as ph:
            for record in reader:
                for k, v in record.items():
                    if k == 'course_id':
                        record[k] = downutils.get_sql_course_id(v or '')
                    if k == 'module_id':
                        record[k] = module_from_block(v or '')
                    if (v or '').lower() == 'null':
                        record[k] = None
                check_record_schema(record, module_schema)
                zh.write(json.dumps(record) + '\n')
                try:
                    state = json.loads(
                        (record.get('state') or '{}').replace('\\\\', '\\')
                    )
                except json.JSONDecodeError:
                    continue
                if not all(k in state for k in prob_cols):
                    continue
                url_name = record.get('module_id', '').split('/')[-1]
                panalysis = make_problem_analysis(
                    state, course_id=record.get('course_id'),
                    user_id=record.get('student_id'),
                    problem_url_name=url_name,
                    grade=record.get('grade', 0),
                    max_grade=record.get('max_grade', 0),
                    created=record.get('created')
                )
                check_record_schema(panalysis, problem_schema)
                ph.write(json.dumps(panalysis) + '\n')
            zh.flush()
            ph.flush()


def _default_roles():
    """
    Generate a default roles record to give to a defaultdict
    """
    return {
        'course_id': None, 'user_id': None, 'roles_isBetaTester': 0,
        'roles_isInstructor': 0, 'roles_isStaff': 0,
        'roles_isCCX': 0, 'roles_isFinance': 0,
        'roles_isLibrary': 0, 'roles_isSales': 0,
        'forumRoles_isAdmin': 0, 'forumRoles_isCommunityTA': 0,
        'forumRoles_isModerator': 0, 'forumRoles_isStudent': 0,
        'roles': None,
    }


def make_roles_table(
    dirname,
    schema_dir=SCHEMA_DIR,
    outname='roles.json.gz'
):
    """
    Generate a file to be loaded into the roles table of a dataset

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type schema_dir: Union[None, str]
    :param schema_dir: Directory where schema files live
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing, but writes the generated data to the target files
    """
    schema_dir = schema_dir or SCHEMA_DIR
    files = {
        'student_courseaccessrole-analytics.sql',
        'django_comment_client_role_users-analytics.sql',
    }
    roles = {
        'beta_testers': 'roles_isBetaTester',
        'ccx_coach': 'roles_isCCX',
        'finance_admin': 'roles_isFinance',
        'instructor': 'roles_isInstructor',
        'library_user': 'roles_isLibrary',
        'sales_admin': 'roles_isSales',
        'staff': 'roles_isStaff',
        'Administrator': 'forumRoles_isAdmin',
        'Community': 'forumRoles_isCommunityTA',
        'Community TA': 'forumRoles_isCommunityTA',
        'Moderator': 'forumRoles_isModerator',
        'Student': 'forumRoles_isStudent',
    }
    with gzip.open(os.path.join(dirname, outname), 'wt') as zh:
        data = defaultdict(_default_roles)
        for file_ in map(lambda f: os.path.join(dirname, f), files):
            if not os.path.exists(file_):
                raise OSError(
                    '{f} does not exist in the SQL bundle.'.format(f=file_)
                )
            with open(file_) as fh:
                line = fh.readline().replace('\tname', '\trole')
                header = []
                for c in line.split('\t'):
                    header.append(c.strip())
                reader = csv.DictReader(
                    fh, delimiter='\t', quotechar='\'', lineterminator='\n',
                    fieldnames=header
                )
                for inrow in reader:
                    outrow = data[inrow.get('user_id')]
                    outrow['user_id'] = inrow.get('user_id')
                    outrow['course_id'] = downutils.get_sql_course_id(
                        inrow.get('course_id', '')
                    )
                    col = roles.get(inrow.get('role'))
                    if col is not None:
                        outrow[col] = 1
                    if 'Student' in inrow.get('role', ''):
                        rval = 'Student'
                    else:
                        rval = 'Staff'
                    outrow['roles'] = rval
        staff = set(k for k in roles.values() if k.startswith('roles_'))
        for record in data.values():
            if any(record.get(k) for k in staff):
                record['roles'] = 'Staff'
            zh.write(json.dumps(record) + '\n')
        zh.flush()


def make_sql_tables_seq(
    dirnames, verbose=False, logger=None, fail_fast=False, debug=False,
    schema_dir=SCHEMA_DIR,
):
    """
    Given an iterable of SQL directories, make the SQL tables
    defined in this module.
    This convenience function calls all the report generating functions
    for the given directory name

    :type dirnames: Iterable[str]
    :param dirnames: Names of SQL directories
    :type verbose: bool
    :param verbose: Print a message when a report is being made
    :type logger: logging.Logger
    :param logger: A logging.Logger object to print messages with
    :type fail_fast: bool
    :param fail_fast: Whether or not to bail after the first error
    :type debug: bool
    :param debug: Show the stacktrace that caused the error
    :type schema_dir: str
    :param schema_dir: The directory where schema files live
    :rtype: bool
    :return: True if the files are generated, and False otherwise.
    """
    schema_dir = schema_dir or SCHEMA_DIR
    reports = (
        make_course_axis, make_forum_table, make_grades_persistent,
        make_grading_policy, make_roles_table,
        make_student_module, make_user_info_combo,
    )
    fails = []
    for dirname in dirnames:
        for fn in reports:
            tbl = fn.__name__.replace('make_', '').replace('_table', '')
            if verbose and logger is not None:
                msg = 'Making {f} with files in {d}'
                logger.info(msg.format(f=tbl, d=dirname))
            try:
                fn(dirname=dirname, schema_dir=schema_dir)
            except:
                _, excp, tb = sys.exc_info()
                if debug:
                    traces = ['{e}'.format(e=excp)]
                    traces += map(str.strip, traceback.format_tb(tb))
                    excp = '\n'.join(traces)
                msg = (
                    'Error encountered while making the {n} table(s) '
                    'with the given directory {d}: {e}'
                )
                excp = MissingFileException(msg.format(
                    d=dirname, n=tbl, e=excp
                ))
                if fail_fast:
                    raise excp from None
                fails.append(excp)
                _delete_incomplete_matches(dirname, tbl)
        if verbose and logger is not None:
            for failure in fails:
                logger.error(failure)
            logger.info('Done processing files in {d}'.format(d=dirname))
    return not bool(fails)


def make_sql_tables_par(
    dirnames, verbose=False, logger=None, fail_fast=False, debug=False,
    schema_dir=SCHEMA_DIR
):
    """
    Given a list of SQL directories, make the SQL tables
    defined in this module.
    This convenience function calls all the report generating functions
    for the given directory name

    :type dirnames: List[str]
    :param dirnames: Names of SQL directories
    :type verbose: bool
    :param verbose: Print a message when a report is being made
    :type logger: logging.Logger
    :param logger: A logging.Logger object to print messages with
    :type fail_fast: bool
    :param fail_fast: Whether or not to bail after the first error
    :type debug: bool
    :param debug: Show the stacktrace that caused the error
    :type schema_dir: str
    :param schema_dir: The directory where schema files live
    :rtype: bool
    :return: True if the files are generated, and False otherwise.
    """
    schema_dir = schema_dir or SCHEMA_DIR
    reports = (
        make_course_axis, make_forum_table, make_grades_persistent,
        make_grading_policy, make_roles_table,
        make_student_module, make_user_info_combo,
    )
    results = dict()
    nprocs = mp.cpu_count()
    if len(dirnames) < nprocs:
        nprocs = len(dirnames)
    with ProcessPool(nprocs, initializer=_sql_pool_init) as pool:
        for fn in reports:
            for dirname in dirnames:
                tbl = fn.__name__.replace('make_', '').replace('_table', '')
                if verbose and logger is not None:
                    msg = 'Making {f} with files in {d}'
                    logger.info(msg.format(f=tbl, d=dirname))
                results[(tbl, dirname)] = pool.apply_async(
                    fn, args=(dirname, schema_dir)
                )
        fails = []
        for (tbl, dirname), result in results.items():
            try:
                result.get()
            except KeyboardInterrupt:
                logger.error(
                    'Report generation interrupted by user.'
                )
                raise EarlyExitError()
            except:
                _, excp, tb = sys.exc_info()
                if debug:
                    traces = ['{e}'.format(e=excp)]
                    traces += map(str.strip, traceback.format_tb(tb))
                    excp = '\n'.join(traces)
                msg = (
                    'Error encountered while making the {n} table(s) '
                    'with the given directory {d}: {e}'
                )
                excp = MissingFileException(msg.format(
                    d=dirname, n=tbl, e=excp
                ))
                if fail_fast:
                    raise excp from None
                fails.append(excp)
                _delete_incomplete_matches(dirname, tbl)
    if verbose and logger is not None:
        for failure in fails:
            logger.error(failure)
        for dirname in dirnames:
            logger.info('Done processing files in {d}'.format(d=dirname))
    return not bool(fails)


def make_table_from_sql(
    table, course_id, client, project, append=False,
    query_dir=QUERY_DIR, schema_dir=SCHEMA_DIR,
    wait=False, geo_table='geocode.geoip', youtube_table='videos.youtube',
):
    """
    Generate a BigQuery table using the given table name,
    course ID and a matching SQL query file in the query_dir folder.
    The query file contains placeholder for course ID, dataset name and
    other details.

    :type table: str
    :param table: table name
    :type course_id: str
    :param course_id: Course ID whose secondary reports are being generated
    :type client: bigquery.Client
    :param client: An authenticated bigquery.Client object
    :type project: str
    :param project: GCP project id where the video_axis table is loaded.
    :type query_dir: Union[None, str]
    :param query_dir: Directory where query files are saved.
    :type schema_dir: Union[None, str]
    :param schema_dir: Directory where schema files live
    :type geo_table: str
    :param geo_table: Table name in BigQuery with geolocation data for IPs
    :type youtube_table: str
    :param youtube_table: Table name in BigQuery with YouTube video details
    :type wait: bool
    :param wait: Whether to wait for the query job to finish running
    :rtype: Dict[str, Dict[str, str]]
    :return: Returns the errors dictionary from the LoadJob object tied to the query
    """
    schema_dir = schema_dir or SCHEMA_DIR
    query_dir = query_dir or QUERY_DIR
    latest_dataset = uputils.course_to_bq_dataset(
        course_id, 'sql', project
    )
    log_dataset = uputils.course_to_bq_dataset(
        course_id, 'log', project
    )
    try:
        cols = []
        fields, schema_desc = uputils.get_bq_schema(table, schema_dir)
        for f in fields:
            cols.append(uputils.sqlify_bq_field(f))
        cols = ',\n'.join(cols)
    except MissingSchemaException:
        cols = ''
        schema_desc = ''
    query, description = extract_table_query(table, query_dir)
    table = '{d}.{t}'.format(d=latest_dataset, t=table)
    if append:
        config = uputils.make_bq_query_config(append=True, plain=False, table=table)
        config.destination = table
    else:
        config = uputils.make_bq_query_config(plain=True)
        query = BQ_DDL.format(
            table=table,
            description=(schema_desc or description.strip()),
            query=query,
            cols='({c})'.format(c=cols) if cols else ''
        )
    query = Template(query).render(
        geo_table=geo_table, youtube_table=youtube_table, course_id=course_id,
    )
    try:
        job = client.query(
            query.format(
                latest_dataset=latest_dataset, log_dataset=log_dataset,
                geo_table=geo_table, youtube_table=youtube_table,
                course_id=course_id
            ),
            job_id='{t}_{dt}'.format(
                t=table.replace('.', '_'),
                dt=datetime.now().strftime('%Y%m%d%H%M%S')
            ),
            job_config=config,
        )
    except Exception as excp:
        errors = getattr(excp, 'errors', [None])
        if errors:
            msg = errors[0].get('message')
        else:
            msg = str(excp)
        raise SQLQueryException(msg)
    if wait:
        status = wait_for_bq_job_ids([job.job_id], client)
        return status[job.job_id]
    return job.errors or {}


def make_tables_from_sql(
    tables, course_id, client, project, append=False,
    query_dir=QUERY_DIR, wait=False,
    geo_table='geocode.geoip', youtube_table='videos.youtube',
    parallel=False, fail_fast=False,
    schema_dir=SCHEMA_DIR,
):
    """
    This is the plural/multiple tables version of make_table_from_sql

    :type tables: Iterable[str]
    :param tables: BigQuery table names to create or append to
    :type course_id: str
    :param course_id: Course ID whose secondary reports are being generated
    :type client: bigquery.Client
    :param client: An authenticated bigquery.Client object
    :type project: str
    :param project: GCP project id where the video_axis table is loaded.
    :type query_dir: Union[None, str]
    :param query_dir: Directory where query files are saved.
    :type geo_table: str
    :param geo_table: Table name in BigQuery with geolocation data for IPs
    :type youtube_table: str
    :param youtube_table: Table name in BigQuery with YouTube video details
    :type wait: bool
    :param wait: Whether to wait for the query job to finish running
    :type parallel: bool
    :param parallel: Whether the function is running in a process pool
    :type fail_fast: bool
    :param fail_fast: Whether to stop processing after the first error
    :type schema_dir: Union[None, str]
    :param schema_dir: Directory where schema files live
    :rtype: Dict[str, Dict[str, str]]
    :return: Return a dict mapping table names to their corresponding errors
    """
    query_dir = query_dir or QUERY_DIR
    schema_dir = schema_dir or SCHEMA_DIR
    if parallel:
        global report_bq_client
        client = report_bq_client
    out = dict()
    dataset = '{p}.{c}_latest'.format(
        p=project, c=course_id.replace('/', '__').replace('.', '_')
    )
    client.create_dataset(dataset, exists_ok=True)
    for table in tables:
        out[table] = make_table_from_sql(
            table=table, course_id=course_id, client=client, project=project,
            append=append, geo_table=geo_table, wait=wait,
            query_dir=query_dir, youtube_table=youtube_table,
            schema_dir=schema_dir,
        )
        if fail_fast and out[table]:
            return out
    return out


def make_tables_from_sql_par(
    tables, courses, project, append=False, query_dir=QUERY_DIR,
    wait=False, geo_table='geocode.geoip', youtube_table='videos.youtube',
    safile=None, size=mp.cpu_count(), logger=None, fail_fast=False,
    schema_dir=SCHEMA_DIR,
):
    """
    Parallel version of make_tables_from_sql

    :type tables: Iterable[str]
    :param tables: An iterable of BigQuery table names
    :type courses: Iterable[str]
    :param courses: An iterable of course IDs
    :type project: str
    :param project: The GCP project against which queries are run
    :type append: bool
    :param append: Whether to append query results to the target tables
    :type query_dir: str
    :param query_dir: The directories where the SQL query files are found
    :type wait: bool
    :param wait: Whether to wait for the BigQuery load jobs to complete
    :type geo_table: str
    :param geo_table: Table name in BigQuery with geolocation data for IPs
    :type youtube_table: str
    :param youtube_table: Table name in BigQuery with YouTube video details
    :type safile: Union[None, str]
    :param safile: GCP service account file to use to connect to BigQuery
    :type size: int
    :param size: Size of the process pool to run queries in parallel
    :type logger: logging.Logger
    :param logger: A Logger object with which to report steps carried out
    :type fail_fast: bool
    :param fail_fast: Whether to stop processing after the first error
    :type schema_dir: Union[None, str]
    :param schema_dir: Directory where schema files live
    :rtype: Dict[str, Dict[str, Dict[str, str]]]
    :return: A dict mapping course_ids to tables and their query errors
    """
    query_dir = query_dir or QUERY_DIR
    schema_dir = schema_dir or SCHEMA_DIR
    if len(courses) < size:
        size = len(courses)
    results = dict()
    with ProcessPool(
        size, initializer=_report_pool_init,
        initargs=(project, safile)
    ) as pool:
        for course_id in courses:
            if logger:
                logger.info(
                    'Making secondary tables for course ID {cid}'.format(
                        cid=course_id
                    )
                )
            async_result = pool.apply_async(
                func=make_tables_from_sql, kwds=dict(
                    tables=tables, course_id=course_id, client=None,
                    project=project, append=append, query_dir=query_dir, wait=wait,
                    geo_table=geo_table, youtube_table=youtube_table,
                    parallel=True, fail_fast=fail_fast, schema_dir=schema_dir,
                )
            )
            results[course_id] = async_result
        for course_id in results:
            result = results[course_id]
            results[course_id] = result.get()
            if logger:
                logger.info(
                    'All queries submitted for course ID {cid}'.format(
                        cid=course_id
                    )
                )
    return results
