"""
Test the download package
"""
import os
import unittest
import warnings

from simeon.download import (
    aws, logs, utilities as downutils
)
from simeon.exceptions import DecryptionError


class TestDownloadUtilities(unittest.TestCase):
    """
    Test the utility functions and classes in the aws module
    """
    def setUp(self):
        self.dead_letter_text = os.path.join(
            'dead_letters', 'dead_letter_queue'
        )
        self.bad_json_log_lines = [
            """{'time': '2020-01-01 23:45:13'}""",
            """{'time': '2020-01-01 23:45:13', 'course_id': 'science'}""",
        ]
        self.bad_log_lines = [
            """{}""",
            """{"time": "hello-this-is-a-mess", "course_id": ""}""",
            """{
                "time": "2020-01-01 23:45:13",
                "course_id": "MITx/science/3T2020"
            }""",

        ]
        self.good_log_lines = [
            """{
                "time": "2021-01-01 23:45:45",
                "course_id": "MITx/123/1T2020",
                "event": "{}",
                "event_type": "problem"
            }""",
            """{
                "time": "2021-06-01 20:45:45",
                "course_id": "MITx/456/2T2021",
                "event": "{}",
                "event_type": "lecture"
            }"""
        ]
        self.good_email_fnames = [
            'email-opt-in/email-opt-in-mitx-2021-01-01.zip',
            'email-opt-in/email-opt-in-mitx-2021-01-02.zip',
            'email-opt-in/email-opt-in-mitx-2021-01-03.zip',
        ]
        self.bad_email_fnames = [
            'email-opt-in/email-opt-in-mitx-20210101.zip',
            'email-opt-in/email-opt-in-mitx-twentyone-one-one.zip',
            'email-opt-in/email-opt-in-mitx-twentyone-one-two.zip',
            'email-opt-in/email-opt-in-mitx-twentyone-one-three.zip',
        ]
        self.good_module_id_recs = [
            {
                'event': {},
                'event_type': (
                    '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1'
                    ':ORGx+Course1x+4T2099+type@foo+block@bar/handler'
                )
            },
            {
                'event': {},
                'event_type': '',
                'context': {
                    'path': (
                        '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1:'
                        'ORGx+Course1x+4T2099+type@foo+block@bar/handler'
                    )
                }
            },
            {
                'event': {},
                'event_type': 'problem',
                'page': '/courses/course-v1:ORGx+Course1x+4T2099/b'
            },
            {
                'event': {'id': 'input_i4x-ORGx-Course1x-foo-bar_0_a='},
                'event_type': 'problem'
            },
            {
                'event': {'ids': ['input_foo_0_0=']},
                'event_type': 'problem_graded',
                'page': '/courses/course-v1:ORGx+Course1x+4T2099/b'
            },
            {
                'event': {'id': 'input_i4x-ORGx-Course1x-foo-bar_0_a='},
                'event_type': 'problem_graded'
            },
            {
                'event': {},
                'event_type': (
                    '/courses/course-v1:ORGx+Course1x+4T2099/xblock/'
                    'block-v1:a+a+a+type@foo+block@bar'
                )
            },
            {
                'event': {},
                'event_type': '',
                'context': {
                    'path': (
                        '/courses/course-v1:ORGx+Course1x+4T2099/'
                        'xblock/block-v1:a+a+a+type@foo+block@bar'
                    )
                }
            },
            {
                'event': {
                    'id': 'block-v1:ORGx+Course1x+4T2099+type@foo+block@bar'
                },
                'event_type': ''
            },
            {
                'event': {'id': 'i4x-ORGx-Course1x-foo-bar'},
                'event_type': ''
            },
            {
                'event': {'id': (
                    '/courses/course-v1:ORGx+Course1x+4T2099'
                    '/courseware/chapter0/bar/'
                )},
                'event_type': 'play_video',
                'page': (
                    '/courses/course-v1:ORGx+Course1x+4T2099'
                    '/courseware/chapter0/bar/'
                )
            },
            {
                'event': {
                    'id': 'block-v1:ORGx+Course1x+4T2099+type@foo+block@bar'
                },
                'event_type': ''
            },
            {
                'event': {'id': 'i4x://ORGx/Course1x/foo/bar'},
                'event_type': '',
                'event_source': 'browser'
            },
            {
                'event': {
                    'id': 'i4x://ORGx/Course1x/foo/bar',
                    'new': 'baz'
                },
                'event_type': 'seq_goto',
                'event_source': 'browser'
            },
            {
                'event': {
                    'id': 'i4x://ORGx/Course1x/foo/bar',
                    'new': 'baz'
                },
                'event_type': 'seq_next',
                'event_source': 'browser'
            },
            {
                'event': {},
                'event_type': 'page_close',
                'event_source': 'browser',
                'page': '/courses/ORGx/Course1x/4T2099/courseware/foo/bar/'
            },
            {
                'event': {},
                'event_type': 'page_close',
                'event_source': 'browser',
                'page': '/courses/ORGx/Course1x/4T2099/courseware/foo/'
            },
            {
                'event': {
                    'id': 'input_i4x-ORGx-Course1x-problem-foo_0_0='
                },
                'event_type': '',
                'event_source': 'browser',
            },
            {
                'event': {
                    'id': 'input_i4x-ORGx-Course1x-problem-foo_0_0='
                },
                'event_type': '',
                'event_source': 'browser',
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/discussion/threads/foo'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/discussion/'
                    'forum/i4xfoo/threads/bar'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/'
                    'discussion/i4xfoo/threads/create'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/'
                    'discussion/forum/foo/threads/bar'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/courseware/foo/bar/'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/courseware/foo/'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': '/courses/ORGx/Course1x/4T2019/jump_to_id/foo',
                'event_source': ''
            },
            {
                'event': {},
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/xblock/'
                    'i4x:;_;_a;_a;_foo;_bar/handler/'
                ),
                'event_source': ''
            },
            {
                'event': {},
                'event_type': '',
                'event_source': '',
                'context': {
                    'path': (
                        '/courses/ORGx/Course1x/4T2019/xblock/'
                        'i4x:;_;_a;_a;_foo;_bar/handler/'
                    )
                }
            },
            {
                'event': {'id': 'input_i4x-ORGx-Course1x-foo-bar_0_a='},
                'event_type': '',
                'event_source': ''
            },
            {
                'event': {},
                'event_type': 'i4x://ORGx/Course1x/foo/bar',
                'event_source': ''
            },
            {
                'event': {
                    'POST': {'position': ['baz']}
                },
                'event_type': 'i4x://ORGx/Course1x/foo/bar/goto_position',
                'event_source': ''
            },
            {
                'event': {},
                'event_type': 'i4x://ORGx/Course1x/foo/bar/baz',
                'event_source': ''
            },
            {
                'event': {
                    'id': 'i4x://ORGx/Course1x/foo/bar'
                },
                'event_type': '',
                'event_source': ''
            },
            {
                'event': {'problem_id': 'i4x://ORGx/Course1x/foo/bar'},
                'event_type': '',
                'event_source': ''
            },
            {
                'event': {'id': 'i4x-ORGx-Course1x-video-foo'},
                'event_type': '',
                'event_source': ''
            },
        ]
        self.bad_module_id_recs = [
            {
                'event': dict(),
                'event_type': '',
                'event_source': ''
            },
            {
                'event': '',
                'event_type': 'add_resource'
            },
            {
                'event': '',
                'event_type': 'delete_resource'
            },
            {
                'event': '',
                'event_type': 'recommender_upvote'
            },
            {
                'event': {'id': None},
                'event_type': ''
            },
            {
                'event': '',
                'event_type': '',
                'event_source': ''
            },
        ]
        self.good_sql_course_ids = [
            'filename:MITx+CourseX+9T9999',
            'file:more_file:ORGx+Course.1x+1T1000',
            'file+description:MITx+Course3x+3T3333'
        ]
        self.sql_file_name_directory = 'this/is/not/a/filename/'
        self.sql_file_unexpected_ext = 'prod-foo-bar-baz-bif.doc.gpg'
        self.sql_file_names = [
            {
                'input': 'prod-foo-bar-baz-bif.gz.gpg',
                'good-output': "baz/prod-foo/bar-bif.gz.gpg",
            },
            {
                'input': 'prod-foo-bar-baz-bif.failed.gpg',
                'good-output': "baz/prod-foo/bar-bif.failed.gpg",
            },
            {
                'input': 'prod-foo-bar-baz-bif.json.gpg',
                'good-output': 'baz/prod-foo/bar-bif.json.gpg',
            },
            {
                'input': 'prod-foo-bar-baz-bif.sql.gpg',
                'good-output': 'baz/prod-foo/bar-bif.sql.gpg',
            },
            {
                'input': 'ora/prod-foo-bar-baz-bif.gz.gpg',
                'good-output': "baz/prod-foo/ora/bar-bif.gz.gpg",
            },
            {
                'input': 'prod-foo.mongo.gpg',
                'good-output': 'foo/prod/forum.mongo.gpg'
            }
        ]
        self.course_id_records = [
            {
                'course_id': 'MITx+CourseX+1T2000'
            },
            {
                'context': {
                    'course_id': 'MITx+CourseX+1T2000'
                }
            },
            {
                'event_source': 'browser',
                'page': 'https://edx.org/MITx+CourseX+1T2020'
            },
            {
            'event_type': 'https://edx.org/MITx+CourseX+1T2020'
            },
            {
            'event_type': 'https://edx.org/courses:v1:MITx+CourseX+1T2020/'
            }
        ]

    def test_good_file_dates(self):
        """
        Test that get_file_date can pluck out dates from
        file names with dates in the format YYYY-mm-dd
        """
        fdates = [aws.get_file_date(f) for f in self.good_email_fnames]
        self.assertListEqual(
            fdates, ['2021-01-01', '2021-01-02', '2021-01-03']
        )

    def test_bad_file_dates(self):
        """
        Test that get_file_date returns an empty when given a path
        with no date in it.
        """
        fdates = [aws.get_file_date(f) for f in self.bad_email_fnames]
        self.assertListEqual(fdates, ['', '', '', ''])

    def test_bad_decryption(self):
        """
        Test that decrypt_files raises DecryptionError when file is missing
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.assertRaises(DecryptionError):
                downutils.decrypt_files(
                    'thereisnowaythisfileexists.gpg', False
                )

    def test_good_module_id_recs(self):
        """
        Test that downutils.get_module_id works with valid records
        """
        for record in self.good_module_id_recs:
            msg = 'Getting module ID of {r}'.format(r=record)
            with self.subTest(msg):
                self.assertIsNotNone(downutils.get_module_id(record))

    def test_bad_module_id_records(self):
        """
        Test that downutils.get_module_id returns None with valid records
        """
        for record in self.bad_module_id_recs:
            msg = 'Getting module ID of {r}'.format(r=record)
            with self.subTest(msg):
                self.assertIsNone(downutils.get_module_id(record))

    def test_bad_json_log_lines(self):
        """
        Test that process_line returns the given string along with
        target file name 'dead_letter_queue.json.gz' when the JSON line
        is not properly formatted.
        """
        for i, line in enumerate(self.bad_json_log_lines, 1):
            msg = 'Testing process_line with record {r}'
            with self.subTest(msg.format(r=line)):
                out = logs.process_line(line, i)
                self.assertIn(
                    self.dead_letter_text,
                    out.get('filename', '')
                )
                self.assertEqual(line, out.get('data'))

    def test_bad_log_lines(self):
        """
        Test that process_line returns the original line to be put
        in a dead letter queue when there is no 'event' or 'event_type' in
        the deserialized JSON record.
        """
        for i, line in enumerate(self.bad_log_lines, 1):
            msg = 'Testing process_line with record {r}'
            with self.subTest(msg.format(r=line)):
                out = logs.process_line(line, i)
                self.assertIn(
                    self.dead_letter_text,
                    out.get('filename', '')
                )
                self.assertEqual(line, out.get('data'))

    def test_good_log_lines(self):
        """
        Test that process_line returns a valid dict and a good file name
        when the given line is without issues.
        """
        for i, line in enumerate(self.good_log_lines, 1):
            msg = 'Testing process_line with record {r}'
            with self.subTest(msg.format(r=line)):
                out = logs.process_line(line, i)
                self.assertNotIn(
                    self.dead_letter_text,
                    out.get('filename', '')
                )
                self.assertIsInstance(out.get('data'), dict)

    def test_good_sql_course_ids(self):
        """
        test that get_sql_course_id returns what we would expect
        without issues
        """
        for course_str in self.good_sql_course_ids:
            msg = 'Testing get_sql_course_id with {c}'.format(c=course_str)
            with self.subTest(msg):
                self.assertIsNotNone(downutils.get_sql_course_id(course_str))

    def test_sql_filename_directory(self):
        """
        should just return a tuple of Nones if the given name is a directory
        """
        self.assertEqual(
            downutils.format_sql_filename(self.sql_file_name_directory),
            (None, None)
        )

    def test_unexpected_extension(self):
        """
        if the file extension is not one of the standard ones, raises error
        """
        with self.assertRaises(ValueError):
            downutils.format_sql_filename(self.sql_file_unexpected_ext)

    def test_sql_filenames(self):
        """
        test a variety of filename conversions
        """
        for input_dict in self.sql_file_names:
            msg = 'Testing format_sql_filename with {f}'.format(
                f=input_dict['input']
            )
            with self.subTest(msg):
                self.assertEqual(
                    downutils.format_sql_filename(input_dict['input']),
                    (input_dict['input'], input_dict['good-output'])
                )

    def test_course_ids(self):
        """
        test a variety of JSON-like records for course_id extraction
        """
        for record in self.course_id_records:
            msg = 'Testing get_course_id with {r}'.format(r=record)
            with self.subTest(msg):
                self.assertIsNotNone(downutils.get_course_id(record))

if __name__ == '__main__':
    unittest.main()
