"""
Utility functions and classes to help with making course reports like user_info_combo, person_course, etc.
"""
import csv
import gzip
import json
import os
from collections import OrderedDict
from functools import reduce
from multiprocessing.pool import ThreadPool

from simeon.download import utilities as downutils
from simeon.exceptions import MissingSchemaException


csv.field_size_limit(13107200)
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
    keys = list(record)
    for k in keys:
        if not isinstance(record.get(k), dict):
            if k not in (f.get('name') for f in schema):
                del record[k]
        else:
            subrecord = record.get(k, {})
            target = next((f for f in schema if f.get('name') == k), None)
            if target is None:
                subfields = []
            else:
                subfields = target.get('fields')
            drop_extra_keys(subrecord, subfields)


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
            for col in rfh.readline().split('\t'):
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
                target = users.get(row.get(uid_col))
                if target is None:
                    continue
                tmp = dict((k, row.get(k)) for k in cols)
                target.update(tmp)
    outcols = reduce(lambda l, r: l + r, USER_INFO_COLS.values())
    outcols += ADDED_COLS
    with gzip.open(os.path.join(dirname, outname), 'wt') as zh:
        for record in users.values():
            outrow = dict()
            for k in outcols:
                val = record.get(k) or ''
                if 'course_id' in k:
                    val = downutils.get_sql_course_id(val) if val else ''
                if 'certificate_grade' in k:
                    try:
                        val = str(float(val))
                    except (TypeError, ValueError):
                        val = ''
                if val == 'NULL' or val == 'null':
                    outrow[k] = ''
                else:
                    outrow[k] = val
            id_cols = ('user_id', 'certificate_user_id')
            if all(not outrow.get(k) for k in id_cols):
                continue
            check_record_schema(outrow, schema, True)
            drop_extra_keys(outcols, schema)
            zh.write(json.dumps(outrow) + '\n')


def batch_user_info_combos(
    dirnames, outname='user_info_combo.json.gz',
    verbose=False, logger=None
):
    """
    Call make_user_info_combo in a ThreadPool

    :type dirnames: Iterable[str]
    :param dirnames: Iterable of course directories
    :type outname: str
    :param outname: The filename to give it to a generated report
    :type verbose: bool
    :param verbose: Print a message when a report is being made
    :type logger: logging.Logger
    :param logger: A logging.Logger object to print messages with
    :rtype: None
    :return: Nothing
    """
    with ThreadPool(10) as pool:
        results = dict()
        for dirname in dirnames:
            if verbose and logger is not None:
                msg = 'Making a user info combo report with files in {d}'
                logger.info(msg.format(d=dirname))
            async_result = pool.apply_async(
                    func=make_user_info_combo, kwds=dict(
                        dirname=dirname, outname=outname,
                    )
            )
            results[async_result] = dirname
        for result in results:
            result.get()
            if verbose and logger is not None:
                msg = 'Report generated for files in {d}'
                logger.info(msg.format(d=dirname))
