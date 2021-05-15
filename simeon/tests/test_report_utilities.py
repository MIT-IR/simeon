"""
Test the report package
"""
import glob
import json
import os
import unittest

import simeon.report.utilities as rutils


class TestReportUtilities(unittest.TestCase):
    """
    Test the utility and report generating functions
    in the simeon.report package
    """
    def setUp(self):
        self.blocks = [
            (
                "/courses/course-v1:MITx+0.502x+3T2019/"
                "xblock/block-v1:MITx+0.502x+3T2019+type@"
                "poll+block@d9c185016d32493e82239a541f7a274c/"
                "handler/student_voted"
            ),
            (
                "/courses/course-v1:MITx+0.502x+3T2019/xblock/"
                "block-v1:MITx+0.502x+3T2019+type@video+block@"
                "9636c575fdcf48e5b4dd54abbbd4ed28/handler_noauth/"
                "transcript/download"
            )
        ]
        self.courses = [
            'MITx/0.502x/3T2019', 'MITx/0.502x/3T2019',
        ]
        self.modules = [
            'MITx/0.502x/3T2019/poll/d9c185016d32493e82239a541f7a274c',
            'MITx/0.502x/3T2019/video/9636c575fdcf48e5b4dd54abbbd4ed28'
        ]
        self.fixtures_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'fixtures',
        )
        self.records = [
            {},
            {'user_id': 123, 'username': 'helloworld'},
            {'user_id': 321,},
        ]
        self.extra_records = [
            {
                'actor_name': 'Nova Jovic',
                'name': {
                    'first': 'Mella',
                    'last': 'Jameel'
                },
            },
            {
                'friends': ['Dior', 'Chanel'],
                'name': 'Cher',
            },
            {},
            {'user_id': 123, 'username': '123'},
        ]
        self.sfiles = [
            'schema_user_info_combo.json', 'schema_course_axis.json'
        ]
        self.schemas = []
        for f in self.sfiles:
            tbl = f.replace('schema_', '').replace('.json', '')
            f = os.path.join(rutils.SCHEMA_DIR, f)
            with open(f) as fh:
                self.schemas.append(json.load(fh).get(tbl))

    def tearDown(self):
        files = glob.glob(os.path.join(self.fixtures_dir, '*'))
        for file_ in files:
            try:
                os.remove(file_)
            except:
                continue
    
    def test_check_record_schema(self):
        """
        Test the check_record_schema function
        with records missing various fields
        """
        for record in self.records:
            msg = (
                'Test check_record_schema with {r} and coerce=True'
                ' and the schema files {f}'
            )
            with self.subTest(msg.format(r=record, f=', '.join(self.sfiles))):
                for schema in self.schemas:
                    copied = record.copy()
                    rutils.check_record_schema(
                        copied, schema, True, True
                    )
                    self.assertTrue(len(copied) > len(record))
            msg = (
                'Test check_record_schema with {r} and coerce=False'
                ' and the schema files for {f}'
            )
            with self.subTest(msg.format(r=record, f=', '.join(self.sfiles))):
                with self.assertRaises(rutils.SchemaMismatchException):
                    for schema in self.schemas:
                        updated = rutils.check_record_schema(
                            record.copy(), schema, False, True
                        )
    
    def test_drop_extra_keys(self):
        """
        Test the drop_extra_keys function with records
        that have extra fields.
        """
        for record in self.extra_records:
            msg = (
                'Test drop_extra_keys with {r} '
                ' and the schema files {f}'
            )
            with self.subTest(msg.format(r=record, f=', '.join(self.sfiles))):
                for schema in self.schemas:
                    copied = record.copy()
                    rutils.drop_extra_keys(copied, schema)
                    self.assertTrue(len(record) >= len(copied))
    
    def test_missing_extract_table_query(self):
        """
        Test the extract_table_query function for a table
        that does not exist.
        """
        with self.assertRaises(rutils.MissingQueryFileException):
            rutils.extract_table_query(
                'Idonotexistfornowbutmaybesomeday',
                rutils.QUERY_DIR,
            )
    
    def test_good_extract_table_query(self):
        """
        Test the extract_table_query function for person_course.
        """
        results = rutils.extract_table_query(
            'person_course',
            rutils.QUERY_DIR,
        )
        self.assertTrue(all(results))

    def test_make_sql_tables(self):
        """
        Test the make_sql_tables function with a directory with no files
        """
        with self.assertRaises(rutils.MissingFileException):
            rutils.make_sql_tables_seq(self.fixtures_dir, fail_fast=True)
    
    def test_make_course_axis(self):
        """
        Test the make_course_axis function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_course_axis(self.fixtures_dir)

    def test_make_forum_table(self):
        """
        Test the make_forum_table function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_forum_table(self.fixtures_dir)
    
    def test_make_grades_persistent(self):
        """
        Test the make_grades_persistent function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_grades_persistent(self.fixtures_dir)

    def test_make_grading_policy(self):
        """
        Test the make_grading_policy function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_grading_policy(self.fixtures_dir)

    def test_make_roles_table(self):
        """
        Test the make_roles_table function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_roles_table(self.fixtures_dir)

    def test_make_student_module(self):
        """
        Test the make_student_module function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_student_module(self.fixtures_dir)

    def test_make_user_info_combo(self):
        """
        Test the make_user_info_combo function inside simeon.report.utilities
        with an empty directory
        """
        with self.assertRaises(OSError):
            rutils.make_user_info_combo(self.fixtures_dir)

    def test_module_from_block(self):
        """
        Test the module_from_block function
        """
        for block, module in zip(self.blocks, self.modules):
            msg = 'Testing module_from_block with {b}'
            with self.subTest(msg.format(b=block)):
                self.assertEqual(
                    rutils.module_from_block(block), module
                )

    def test_course_from_block(self):
        """
        Test the course_from_block function
        """
        for block, course in zip(self.blocks, self.courses):
            msg = 'Testing course_from_block with {b}'
            with self.subTest(msg.format(b=block)):
                self.assertEqual(
                    rutils.course_from_block(block), course
                )
