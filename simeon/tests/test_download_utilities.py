"""
Test the download module in the download package
"""
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
                'event': '',
                'event_type': (
                    '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1'
                    ':ORGx+Course1x+4T2099+type@foo+block@bar/handler'
                )
            },
            {
                'event': '',
                'event_type': '',
                'context': {
                    'path': (
                        '/courses/course-v1:ORGx+Course1x+4T2099/xblock/block-v1:'
                        'ORGx+Course1x+4T2099+type@foo+block@bar/handler'
                    )
                }
            },
            {
                'event': 'input_foo_0_0=',
                'event_type': 'problem',
                'page': '/courses/course-v1:ORGx+Course1x+4T2099/b'
            },
            {
                'event': 'input_i4x-ORGx-Course1x-foo-bar_0_a=',
                'event_type': 'problem'
            },
            {
                'event': ['input_foo_0_0='],
                'event_type': 'problem_graded',
                'page': '/courses/course-v1:ORGx+Course1x+4T2099/b'
            },
            {
                'event': ['input_i4x-ORGx-Course1x-foo-bar_0_a='],
                'event_type': 'problem_graded'
            },
            {
                'event': '',
                'event_type': (
                    '/courses/course-v1:ORGx+Course1x+4T2099/xblock/'
                    'block-v1:a+a+a+type@foo+block@bar'
                )
            },
            {
                'event': '',
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
                'event': {'id': 'foo'},
                'event_type': 'play_video',
                'page': (
                    '/courses/course-v1:ORGx+Course1x+4T2099'
                    '/courseware/chapter0/bar/'
                )
            },
            {
                'event': 'block-v1:ORGx+Course1x+4T2099+type@foo+block@bar',
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
                'event': '',
                'event_type': 'page_close',
                'event_source': 'browser',
                'page': '/courses/ORGx/Course1x/4T2099/courseware/foo/bar/'
            },
            {
                'event': '',
                'event_type': 'page_close',
                'event_source': 'browser',
                'page': '/courses/ORGx/Course1x/4T2099/courseware/foo/'
            },
            {
                'event': 'input_i4x-ORGx-Course1x-problem-foo_0_0=',
                'event_type': '',
                'event_source': 'browser',
            },
            {
                'event': ['input_i4x-ORGx-Course1x-problem-foo_0_0='],
                'event_type': '',
                'event_source': 'browser',
            },
            {
                'event': '',
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/discussion/threads/foo'
                ),
                'event_source': ''
            },
            {
                'event': '',
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/discussion/'
                    'forum/i4xfoo/threads/bar'
                ),
                'event_source': ''
            },
            {
                'event': '',
                'event_type': '/courses/ORGx/Course1x/4T2019/discussion/i4xfoo/threads/create',
                'event_source': ''
            },
            {
                'event': '',
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/'
                    'discussion/forum/foo/threads/bar'
                ),
                'event_source': ''
            },
            {
                'event': '',
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/courseware/foo/bar/'
                ),
                'event_source': ''
            },
            {
                'event': '',
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/courseware/foo/'
                ),
                'event_source': ''
            },
            {
                'event': '',
                'event_type': '/courses/ORGx/Course1x/4T2019/jump_to_id/foo',
                'event_source': ''
            },
            {
                'event': '',
                'event_type': (
                    '/courses/ORGx/Course1x/4T2019/xblock/'
                    'i4x:;_;_a;_a;_foo;_bar/handler/'
                ),
                'event_source': ''
            },
            {
                'event': '',
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
                'event': 'input_i4x-ORGx-Course1x-foo-bar_0_a=',
                'event_type': '',
                'event_source': ''
            },
            {
                'event': '',
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
                'event': '',
                'event_type': 'i4x://ORGx/Course1x/foo/bar/baz',
                'event_source': ''
            },
            {
                'event': 'i4x://ORGx/Course1x/foo/bar',
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
                self.assertEqual(
                    out.get('filename', ''), 'dead_letter_queue.json.gz'
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
                self.assertEqual(
                    out.get('filename', ''), 'dead_letter_queue.json.gz'
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
                    'dead_letter_queue.json.gz', out.get('filename', '')
                )
                self.assertIsInstance(out.get('data'), dict)


if __name__ == '__main__':
    unittest.main()

