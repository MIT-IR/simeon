"""
Unit tests for the upload package
"""
import unittest

from simeon.upload import utilities as uputils
from simeon.exceptions import (
    BigQueryNameException, MissingSchemaException
)


class TestUploadUtilities(unittest.TestCase):
    """
    Test the utility functions and classes in the upload package
    """
    def setUp(self):
        self.good_file_mames = [
            ('log', 'data/MITx__123__2T2020/tracklog-2020-06-01.json.gz'),
            ('sql', 'data/MITx__123__2T2020/user_info_combo.csv.gz'),
            ('sql', 'data/MITx__123__2T2020/person_course.csv.gz'),
            ('sql', 'data/MITx__123__2T2020/forum.csv.gz'),
            ('sql', 'data/MITx__123__2T2020/forum_person.csv.gz'),
            ('email', 'data/MITx__123__2T2020/email-opt-in.csv.gz'),
        ]
        self.bad_file_names = [
            ('log', 'data/MITx__123__2T2020/no-date-here.json.gz'),
            ('sql', 'data/MITx__123__2T2020/.hidden.csv.gz')
        ]
        self.bad_file_types = [
            ('bad', 'data/MITx__123__2T2020/tracklog-2020-06-01.json.gz'),
            ('worse', 'data/MITx__123__2T2020/user_info_combo.csv.gz'),
            ('worst', 'data/MITx__123__2T2020/person_course.csv.gz'),
            ('bad', 'data/MITx__123__2T2020/forum.csv.gz'),
            ('worse', 'data/MITx__123__2T2020/forum_person.csv.gz'),
            ('worst', 'data/MITx__123__2T2020/email-opt-in.csv.gz'),
        ]
        self.missing_schema_tables = [
            'this_table_doesnt_exist_anywhere',
            'neither_does_this',
        ]
        self.project = 'our-awesome-project'
        self.bucket = 'our-awesome-bucket'
    
    def test_good_table_names(self):
        """
        Test valid BigQuery table names from file names
        """
        for ftype, fname in self.good_file_mames:
            msg = 'With file name {f} and type {t}'.format(f=fname, t=ftype)
            with self.subTest(msg):
                out = uputils.local_to_bq_table(fname, ftype, self.project)
                self.assertTrue(len(out.split('.')) == 3)
    
    def test_bad_table_names(self):
        """
        Test that badly named data files can't make BigQuery table names
        """
        for ftype, fname in self.bad_file_names:
            msg = 'With file name {f} and type {t}'.format(f=fname, t=ftype)
            with self.subTest(msg):
                with self.assertRaises(BigQueryNameException):
                    uputils.local_to_bq_table(fname, ftype, self.project)
    

    def test_missing_schema(self):
        """
        Test that calling uputils.get_bq_schema for a table with missing
        schema file causes MissingSchemaException
        """
        for table in self.missing_schema_tables:
            with self.assertRaises(MissingSchemaException):
                uputils.get_bq_schema(table)

    def test_local_to_bq_table_with_unknown_file_types(self):
        """
        Test that uputils.local_to_bq_table raises a ValueError when
        given an unknown file type.
        """
        for ftype, fname in self.bad_file_types:
            msg = 'With file name {f} and ftype {t}'.format(f=fname, t=ftype)
            with self.subTest(msg):
                with self.assertRaises(ValueError):
                    uputils.local_to_bq_table(fname, ftype, self.project)


if __name__ == '__main__':
    unittest.main()
