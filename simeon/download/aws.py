"""
Module of utilities to help with listing and downloading files from S3
"""
import json
import os
import re
import sys
import weakref
import zipfile
from datetime import datetime

import boto3 as boto

from simeon.exceptions import (
    AWSException, DecryptionError
)
from simeon.download.utilities import decrypt_files


BUCKETS = {
    'email': {
        'Bucket': 'course-data',
        'Prefix': 'email-opt-in/email-opt-in-{org}-',
    },
    'sql': {
        'Bucket': 'course-data',
        'Prefix': '{org}-',
    },
    'log': {
        'Bucket': 'edx-course-data',
        'Prefix': '{org}/{site}/events/{year}/{org}-{site}-events-',
    },
    'rdx': {
        'Bucket': 'edx-course-data',
        'Prefix': '{org}/rdx/{request}',
    },
}
BEGIN_DATE = '2012-09-01'
END_DATE = datetime.today().strftime('%Y-%m-%d')
DATE_PATT = re.compile(r'\d{4}-\d{2}-\d{2}')
O2B_MAP = dict(
    key='name', last_modified='last_modified', size='size'
)


def make_s3_bucket(bucket, client_id=None, client_secret=None):
    """
    Make a simple boto3 Bucket object pointing to S3
    """
    try:
        resource = boto.resource(
            's3', aws_access_key_id=client_id,
            aws_secret_access_key=client_secret
        )
        return resource.Bucket(bucket)
    except Exception as excp:
        raise AWSException(excp)


def get_file_date(fname):
    """
    Get the date in the name of the S3 blob
    """
    fname = os.path.basename(fname)
    match = DATE_PATT.search(fname)
    if match:
        return match.group(0)
    return ''


class S3Blob():
    """
    A class to represent S3 blobs
    """
    def __init__(self, name, size, last_modified, bucket, local_name=None):
        """
        :type name: str
        :param name: Full path to the object without the bucket name
        :type size: int
        :param size: Size of the object in bytes
        :type last_modified: datetime
        :param last_modified: Last time the object was modified
        :type local_name: Union[None, str]
        :param local_name: Localized file name
        :type bucket: s3.Bucket
        :param bucket: The boto3.s3.Bucket object to tie to this blob
        """
        self.name = name
        self.size = size
        self.last_modified = last_modified
        self.bucket = weakref.proxy(bucket)
        self.bucket = bucket
        if not local_name:
            local_name = self._make_local(name)
        self.local_name = local_name

    @classmethod
    def from_prefix(cls, bucket, prefix):
        """
        Fetch a list of S3Blob objects from AWS whose names
        have the given prefix.

        :type bucket: s3.Bucket
        :param bucket: The boto3.s3.Bucket object to tie to this blob
        :type prefix: str
        :param prefix: A string with which to filter the list of objects
        :rtype: List[S3Blob]
        :return: A list of S3Blob objects
        :raises: AWSException
        """
        out = []
        maps = O2B_MAP
        try:
            matches = bucket.objects.filter(Prefix=prefix)
            for obj in matches:
                details = dict(
                    (v, getattr(obj, k, None)) for k, v in maps.items()
                )
                details['bucket'] = bucket
                out.append(cls(**details))
            return out
        except Exception as excp:
            raise AWSException('{e}'.format(e=excp)) from None

    @classmethod
    def from_info(cls, bucket, type_, date, org='mitx', site='edx'):
        """
        Make a list of blobs with the given parameters

        :type bucket: s3.Bucket
        :param bucket: The boto3.s3.Bucket object to tie to this blob
        :type type_: str
        :param type_: "sql" or "email" or "sql"
        :type date: Union[str, datetime]
        :param date: A datetime or str object for a threshold date
        :type org: str
        :param org: The org whose data will be fetched.
        :type site: str
        :param site: The site from which data were generated
        :rtype: List[S3Blob]
        :raises: AWSException
        """
        prefix = BUCKETS.get(type_, {}).get('Prefix')
        if not prefix:
            msg = (
                'The given file type, {t!r}, does not have any associated'
                ' AWS S3 information.'
            )
            raise AWSException(msg.format(t=type_)) from None
        if isinstance(date, datetime):
            date = date.strftime('%Y-%m-%d')
        year = date[:4]
        month = date[5:7]
        prefix = prefix.format(
            org=org, year=year, site=site, date=date, month=month
        )
        return cls.from_prefix(bucket, prefix)

    @staticmethod
    def _make_local(name):
        """
        Convert the given name into a local name for the file system
        """
        return os.path.join(*name.split('/'))

    def download_file(self, filename=None):
        """
        Download the S3Blob to the local file system
        and return the full path where the file is saved

        :type filename: Union[None, str]
        :param filename: Name of the output file
        :rtype: str
        :return: Returns the full path where the file is saved
        """
        if not filename:
            filename = self.local_name
        dirname, _ = os.path.split(filename)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        self.bucket.download_file(self.name, filename)
        return filename

    def __repr__(self):
        return "Name: {n} - Size: {s} - Last Modified: {m}".format(
            n=self.name, s=self.size, m=self.last_modified
        )

    def to_json(self):
        """
        Jsonify the Blob
        """
        return json.dumps({
            'name': self.name, 'size': self.size,
            'last_modified': self.last_modified.strftime('%c %Z'),
        })
