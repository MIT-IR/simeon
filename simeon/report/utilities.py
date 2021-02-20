"""
Utility functions and classes to help with making course reports like user_info_combo, person_course, etc.
"""
import csv
import gzip
import json
import os
import re
import tarfile
from collections import OrderedDict, defaultdict
from datetime import datetime
from functools import reduce
from xml.etree import ElementTree

from simeon.download import utilities as downutils
from simeon.exceptions import (
    BadSQLFileException, MissingFileException,
    MissingQueryFileException, MissingSchemaException,
)
from simeon.upload import utilities as uputils


csv.field_size_limit(13107200)
BQ_DDL = """#standardSQL
CREATE OR REPLACE TABLE {table}
OPTIONS (
    description = "{description}"
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
    'multiplechoiceresponse', 'numericalresponse',
    'choiceresponse', 'optionresponse', 'stringresponse',
    'formularesponse', 'customresponse','fieldset',
}


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
    done = 0
    while done < len(job_list):
        for job in job_list:
            state = job.done()
            if not state:
                job.reload()
            done += state


def check_record_schema(record, schema, coerce=True):
    """
    Check that the given record matches the same keys found in the given
    schema list of fields. The latter is one of the schemas in
    simeon/upload/schemas/

    :type record: dict
    :param record: Dictionary whose values are modified
    :type schema: Iterable[Dict[str, Union[str, Dict]]]
    :param schema: A list of dicts with info on BigQuery table fields
    :type coerce: bool
    :param coerce: Whether or not to coerce values
    :rtype: None
    :return: Modifies the record if needed
    """
    for field in schema:
        if field.get('field_type') != 'RECORD':
            if field.get('name') not in record:
                if not coerce:
                    raise MissingSchemaException(
                        '{f} is missing from the record'.format(
                            f=field.get('name')
                        )
                    )
                record[field.get('name')] = ''
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


def _extract_table_query(table, query_dir):
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
    :return: A tuple of strings (query string, table description)
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


def make_user_info_combo(dirname, outname='user_info_combo.json.gz'):
    """
    Given a course's SQL directory, make a user_info_combo report

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing
    """
    schema_file = os.path.join(
        SCHEMA_DIR, 'schema_user_info_combo.json'
    )
    with open(schema_file) as sfh:
        schema = json.load(sfh).get('user_info_combo')
    users = dict()
    user_file = 'auth_user-analytics.sql'
    user_cols = USER_INFO_COLS.get((user_file, None))
    with open(os.path.join(dirname, user_file)) as ufh:
        incols = [c.strip() for c in ufh.readline().split('\t')]
        reader = csv.DictReader(
            ufh, delimiter='\t', lineterminator='\n', quotechar='\'',
            fieldnames=incols
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
                    header.append('{p}_{c}'.format(p=prefix, c=col))
                else:
                    uid_col = 'user_id'
                    header.append(col)
            reader = csv.DictReader(
                rfh, delimiter='\t', lineterminator='\n',
                quotechar='\'', fieldnames=header
            )
            for row in reader:
                if uid_col not in row:
                    row[uid_col] = row.get('{p}_id'.format(p=prefix))
                user_id = row.get(uid_col)
                target = users.setdefault(user_id, {})
                target['user_id'] = user_id
                target.update(dict((k, row.get(k)) for k in cols))
    outcols = reduce(lambda l, r: l + r, USER_INFO_COLS.values())
    outcols += ADDED_COLS
    with gzip.open(os.path.join(dirname, outname), 'wt') as zh:
        for record in users.values():
            outrow = dict()
            for k in outcols:
                val = record.get(k)
                if 'course_id' in k:
                    val = downutils.get_sql_course_id(val or '') if val else ''
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
            zh.write(json.dumps(outrow) + '\n')


def course_from_block(block):
    """
    Extract a course ID from the given block ID
    """
    if block.startswith('i4x://'):
        return block.split('//')[-1].replace('course/', '')
    return '/'.join(block.split(':')[-1].split('+', 3)[:3])


def module_from_block(block):
    """
    Extract a module ID from the given block
    """
    if block.startswith('i4x://'):
        return block.lstrip('i4x://')
    segments = block.split(':', 1)[-1].split('+')
    return '/'.join(map(lambda s: s.split('@')[-1], segments))


def get_youtube_id(record):
    """
    Given a course structure record, extract the YouTube ID
    associated with the video element.
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
        problems = [m for m in tf.getmembers() if '/problem/' in m.name]
        for problem in problems:
            if problem.isdir():
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
    """
    meta = record.get('metadata') or dict()
    if 'showanswer' not in meta:
        return False
    return meta['showanswer'] != 'never'


def get_problem_nitems(record):
    """
    Get a value for data.num_items in course_axis
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


def make_course_axis(dirname, outname='course_axis.json.gz'):
    """
    Given a course's SQL directory, make a course_axis report

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing
    """
    fname = os.path.join(dirname, 'course_structure-analytics.json')
    bundle = os.path.join(dirname, 'course-analytics.xml.tar.gz')
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
            'course block. Please reach out to edX to have them fix it.'
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


def make_grades_persistent(
    dirname,
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
    :return: Nothing
    """
    infiles = dict([
        (
            'grades_persistentcoursegrade-analytics.sql',
            first_outname,
        ),
        (
            'grades_persistentsubsectiongrade-analytics.sql',
            second_outname,
        )
    ])
    for file_ in infiles:
        outname = os.path.join(dirname, infiles[file_])
        file_ = os.path.join(dirname, file_)
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
                zh.write(json.dumps(record) + '\n')


def make_grading_policy(dirname, outname='grading_policy.json.gz'):
    """
    Generate a file to be loaded into the grading_policy table
    of the given SQL directory.

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing
    """
    file_ = os.path.join(dirname, 'course-analytics.xml.tar.gz')
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
            'overall_cutoff_for_a', 'overall_cutoff_for_b',
            'overall_cutoff_for_c',
        )
        with gzip.open(outname, 'wt') as zh:
            for grader in grading_policy.get('GRADER', []):
                grader['assignment_type'] = grader.get('type', '')
                grader['name'] = grader.get('type', '')
                grader['fraction_of_overall_grade'] = grader.get('weight')
                for k, v in grading_policy.get('GRADE_CUTOFFS', {}).items():
                    grader['overall_cutoff_for_{k}'.format(k=k.lower())] = v
                zh.write(
                    json.dumps(dict((k, grader.get(k)) for k in cols)) + '\n'
                )


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


def make_forum_table(dirname, outname='forum.json.gz'):
    """
    Generate a file to load into the forum table
    using the given SQL directory

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing
    """
    outname = os.path.join(dirname, outname)
    file_ = os.path.join(dirname, 'forum.mongo')
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
        SCHEMA_DIR, 'schema_forum.json'
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
            drop_extra_keys(record, schema)
            zh.write(json.dumps(record) + '\n')


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
    maps = state.get('correct_map', {})
    answers = state.get('student_answers', {})
    items = []
    for k, v in maps.items():
        items.append(
            {
                'answer_id': k,
                'correctness': v.get('correctness'),
                'correct_bool' : v.get('correctness','') == 'correct',
                'npoints': v.get('npoints'),
                'msg': v.get('msg'),
                'hint': v.get('hint'),
                'response': json.dumps(answers.get(k, '')),
            }
        )
    out = {
        'item': items,
        'attempts': state.get('attempts', 0),
        'done': state.get('done'),
    }
    out.update(extras)
    return out


def make_student_module(dirname, outname='studentmodule.json.gz'):
    """
    Generate files to load into studentmodule and problem_analysis
    using the given SQL directory

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :rtype: None
    :return: Nothing
    """
    outname = os.path.join(dirname, outname)
    second = os.path.join(dirname, 'problem_analysis.json.gz')
    file_ = os.path.join(
        dirname, 'courseware_studentmodule-analytics.sql'
    )
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
                zh.write(json.dumps(record) + '\n')
                try:
                    state = json.loads(
                        record.get('state', '{}').replace('\\\\', '\\')
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
                ph.write(json.dumps(panalysis) + '\n')


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


def make_roles_table(dirname, outname='roles.json.gz'):
    """
    Generate a file to be loaded into the roles table of a dataset
    """
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


def make_sql_tables(dirname, verbose=False, logger=None):
    """
    Given a SQL directory, make the SQL tables
    defined in this module.

    :type dirname: str
    :param dirname: Name of a course's SQL directory
    :type verbose: bool
    :param verbose: Print a message when a report is being made
    :type logger: logging.Logger
    :param logger: A logging.Logger object to print messages with
    :rtype: None
    :return: Nothing
    """
    reports = (
        make_course_axis, make_forum_table, make_grades_persistent,
        make_grading_policy, make_roles_table,
        make_student_module, make_user_info_combo,
    )
    for maker in reports:
        if verbose and logger is not None:
            msg = 'Calling routine {f} on {d}'
            logger.info(msg.format(f=maker.__name__, d=dirname))
        maker(dirname)
        if verbose and logger is not None:
            msg = '{f} made a report from files in {d}'
            logger.info(msg.format(f=maker.__name__, d=dirname))


def make_table_from_sql(
    table, course_id, client, project, append=False,
    query_dir=QUERY_DIR, wait=False,
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
    :type query_dir: str
    :param query_dir: Directory where query files are saved.
    :type wait: bool
    :param wait: Whether to wait for the query job to finish running
    :rtype: bigquery.QueryJob
    """
    latest_dataset = uputils.course_to_bq_dataset(
        course_id, 'sql', project
    )
    log_dataset = uputils.course_to_bq_dataset(
        course_id, 'log', project
    )
    query, description = _extract_table_query(table, query_dir)
    table = '{d}.{t}'.format(d=latest_dataset, t=table)
    if append:
        config = uputils.make_bq_query_config(append=append)
    else:
        config = uputils.make_bq_query_config(plain=True)
        query = BQ_DDL.format(
            table=table,
            description=description.strip(),
            query=query,
        )
    job = client.query(
        query.format(
            latest_dataset=latest_dataset,
            log_dataset=log_dataset,
            course_id=course_id
        ),
        job_id='{t}_{dt}'.format(
            t=table.replace('.', '_'),
            dt=datetime.now().strftime('%Y%m%d%H%M%S')
        ),
        job_config=config,
    )
    if wait:
        wait_for_bq_jobs([job])
    return job