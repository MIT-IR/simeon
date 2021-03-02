"""
Test the report package
"""
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
                'first_name': 'Mella',
                'last_name': 'Jameel',
            },
            {
                'friends': ['Dior', 'Chanel'],
                'name': 'Cher',
            },
            {},
            {'user_id': 123, 'username': '123'},
        ]
        self.sfile = os.path.join(
            rutils.SCHEMA_DIR, 'schema_user_info_combo.json'
        )
        with open(self.sfile) as f:
            self.schema = json.load(f).get('user_info_combo')
    
    def test_check_record_schema(self):
        """
        Test the check_record_schema function
        with records missing various fields
        """
        for record in self.records:
            msg = (
                'Test check_record_schema with {r} and coerce=True'
                ' and the schema file for user_info_combo'
            )
            with self.subTest(msg.format(r=record)):
                copied = record.copy()
                rutils.check_record_schema(
                    copied, self.schema, True
                )
                self.assertTrue(len(copied) > len(record))
            msg = (
                'Test check_record_schema with {r} and coerce=False'
                ' and the schema file for user_info_combo'
            )
            with self.subTest(msg.format(r=record)):
                with self.assertRaises(rutils.SchemaMismatchException):
                    updated = rutils.check_record_schema(
                        record.copy(), self.schema, False
                    )
    
    def test_drop_extra_keys(self):
        """
        Test the drop_extra_keys function with records
        that have extra fields.
        """
        for record in self.extra_records:
            msg = (
                'Test drop_extra_keys with {r} '
                ' and the schema file for user_info_combo'
            )
            with self.subTest(msg.format(r=record)):
                copied = record.copy()
                rutils.drop_extra_keys(copied, self.schema)
                self.assertTrue(len(record) >= len(copied))
    
    def test_extract_table_query(self):
        """
        Test the extract_table_query function for a table
        that does not exist.
        """
        with self.assertRaises(rutils.MissingQueryFileException):
            rutils.extract_table_query(
                'Idonotexistfornowbutmaybesomeday',
                rutils.QUERY_DIR,
            )

    def test_make_sql_tables(self):
        """
        Test the make_sql_tables function with a directory with no files
        """
        with self.assertRaises(rutils.MissingFileException):
            rutils.make_sql_tables(self.fixtures_dir)
