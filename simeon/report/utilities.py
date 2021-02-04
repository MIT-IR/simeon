"""
Utility functions and classes to help with making course reports like user_info_combo, person_course, etc.
"""
import csv
import os
import gzip
from collections import OrderedDict
from functools import reduce


USER_INFO_COLS = OrderedDict([
    (
        'auth_user-analytics.sql',
        [
            'user_id', 'username', 'email',
            'password', 'is_staff', 'last_login',
            'date_joined'
        ],
    ),
    (
        'auth_userprofile-analytics.sql',
        [
            'name', 'language', 'location', 'meta',
            'courseware', 'gender', 'mailing_address',
            'year_of_birth', 'level_of_education', 'goals',
            'allow_certificate', 'country', 'city'
        ],
    ),
    (
        'student_courseenrollment-analytics.sql',
        [
            'course_id', 'created', 'is_active', 'mode'
        ],
    ),
    (
        'certificates_generatedcertificate-analytics.sql',
        [
            'download_url', 'grade', 'course_id', 'key', 'distinction',
            'status', 'verify_uuid', 'download_uuid', 'name', 'created_date',
            'modified_date', 'error_reason', 'mode'
        ],
    ),
    (
        'user_id_map-analytics.sql',
        ['hash_id']
    )
])


def make_user_info_combo(
    dirname, outname='user_info_combo.csv.gz', delim='\t'
):
    """
    Given a course's SQL directory, make a user_info_combo report

    :type dirname: str
    :param dirname: Name of a course's directory of SQL files
    :type outname: str
    :param outname: The filename to give it to the generated report
    :type delim: str
    :param delim: The delimiter of the output file
    :rtype: None
    :return: Nothing
    """
    users = dict()
    user_file = 'auth_user-analytics.sql'
    user_cols = USER_INFO_COLS.get(user_file)
    with open(os.path.join(dirname, user_file)) as ufh:
        incols = [c.strip() for c in ufh.readline().split('\t')]
        reader = csv.DictReader(
            ufh, delimiter='\t', lineterminator='\n', quotechar='"',
            fieldnames=incols
        )
        for row in reader:
            uid = row.get('id')
            users[uid] = dict((k, row.get(k)) for k in user_cols)
    for fname, cols in USER_INFO_COLS.items():
        if fname == user_file:
            continue
        with open(os.path.join(dirname, fname)) as rfh:
            headers = [c.strip() for c in rfh.readline().split('\t')]
            reader = csv.DictReader(
                rfh, delimiter='\t', lineterminator='\n', quotechar='"',
                fieldnames=headers
            )
            for row in reader:
                target = users.get(row.get('user_id'))
                if target is None:
                    continue
                tmp = dict((k, row.get(k)) for k in cols)
                target.update(tmp)
    outcols = reduce(lambda l, r: l + r, USER_INFO_COLS.values())
    with gzip.open(os.path.join(dirname, outname), 'wt') as zh:
        writer = csv.DictWriter(
            zh, delimiter=delim, lineterminator='\n',
            quotechar='"', fieldnames=outcols
        )
        writer.writeheader()
        for record in users.values():
            writer.writerow(
                dict((k, record.get(k)) for k in outcols)
            )
