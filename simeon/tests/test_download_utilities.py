"""
Test the download module in the download package
"""
import unittest
import warnings

from simeon.download import aws
from simeon.exceptions import DecryptionError


class TestDownloadUtilities(unittest.TestCase):
    """
    Test the utility functions and classes in the aws module
    """
    def setUp(self):
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
        Test that decrypt_file raises DecryptionError when file is missing
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.assertRaises(DecryptionError):
                aws.decrypt_file('thereisnowaythisfileexists.gpg', False)


if __name__ == '__main__':
    unittest.main()

