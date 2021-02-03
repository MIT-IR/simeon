"""
Utilities functions and classes to help with loading data to Google Cloud
"""
import glob
import gzip
import os
from datetime import datetime
from typing import List

from google.cloud import bigquery
from google.cloud import storage

from simeon.upload import utilities as uputils

FILE_FORMATS = {
    'log': ['json'],
    'sql': ['csv', 'txt', 'sql']
}


class BigqueryClient(bigquery.Client):
    """
    Subclass bigquery.Client and add convenience methods
    """
    def load_tables_from_dir(
        self, dirname: str, file_type: str, project: str,
        create: bool, append: bool, use_storage: bool=False,
        bucket: str=None
    ) -> List[bigquery.LoadJob]:
        """
        Load all the files in the given directory.

        :type dirname: str
        :param dirname: Grandparent or parent directory of split up files
        :type file_type: str
        :param file_type: One of sql, email, log, rdx
        :type project: str
        :param project: Target GCP project
        :type create: bool
        :param create: Whether or not to create the destination table
        :type append: bool
        :param append: Whether or not to append the records to the table
        :type use_storage: bool
        :param use_storage: Whether or not to load the data from GCS
        :rtype: List[bigquery.LoadJob]
        :return: List of load jobs
        :raises: Propagates exceptions from self.load_table_from_file and
        self.load_table_from_uri
        """
        formats = FILE_FORMATS.get(file_type, [])
        patts = (
            os.path.join(dirname, '*.{f}*.gz'),
            os.path.join(dirname, '*', '*.{f}*.gz')
        )
        files = []
        for patt in patts:
            for format_ in formats:
                files.extend(glob.glob(patt.format(f=format_)))
        jobs = []
        for file_ in files:
            jobs.append(
                self.load_one_file_to_table(
                    file_, file_type, project,
                    create, append, use_storage, bucket,
                )
            )
        return jobs

    def load_one_file_to_table(
        self, fname: str, file_type: str, project: str,
        create: bool, append: bool, use_storage: bool=False,
        bucket: str=None,
    ):
        """
        :type fname: str
        :param fname: The specific file to load
        :type file_type: str
        :param file_type: One of sql, email, log, rdx
        :type project: str
        :param project: Target GCP project
        :type create: bool
        :param create: Whether or not to create the destination table
        :type append: bool
        :param append: Whether or not to append the records to the table
        :type use_storage: bool
        :param use_storage: Whether or not to load the data from GCS
        :type bucket: str
        :param bucket: GCS bucket name to use
        :rtype: bigquery.LoadJob
        :return: The LoadJob object associated with the work being done
        :raises: Propagates exceptions from self.load_table_from_file and
        self.load_table_from_uri
        """
        if use_storage:
            if bucket is None:
                raise ValueError('use_storage=True requires a bucket name')
            loader = self.load_table_from_uri
        else:
            loader = self.load_table_from_file
        format_ = 'json' if file_type == 'log' else 'csv'
        job_prefix = '{t}_data_load_{dt}-'.format(
            t=file_type, dt=datetime.now().strftime('%Y%m%d%H%M%S%f')
        )
        dest = uputils.local_to_bq_table(fname, file_type, project)
        dataset, _ = dest.rsplit('.', 1)
        self.create_dataset(dataset, exists_ok=True)
        if use_storage:
            fname = uputils.local_to_gcs_path(fname, file_type, bucket)
        else:
            fname = gzip.open(fname, 'rb')
        config = uputils.make_bq_config(dest, append, create, format_)
        return loader(
            fname, dest, job_config=config, job_id_prefix=job_prefix
        )


class GCSClient(storage.Client):
    """
    Make a client to load data files to GCS
    """
    def load_on_file_to_gcs(
        self, fname: str, file_type: str, bucket: str, overwrite: bool=True
    ):
        """
        Load the given file to GCS

        :type fname: str
        :param fname: The local file to load to GCS
        :type file_type: str
        :param: file_type: One of sql, email, log, rdx
        :type bucket: str
        :param bucket: GCS bucket name
        :type overwrite: bool
        :param overwrite: Overwrite the target blob if it exists
        :rtype: None
        :return: Nothing
        :raises: Propagates everything from the underlying package
        """
        dest = storage.Blob.from_string(
            uputils.local_to_gcs_path(fname, file_type, bucket),
            client=self
        )
        gen_match = None if overwrite else 0
        dest.upload_from_filename(fname, if_generation_match=gen_match)

    def load_dir(
        self, dirname: str, file_type: str, bucket: str, overwrite: bool=True
    ):
        """
        Load all the files in the given directory or
        any immediate subdirectories

        :type dirname: str
        :param dirname: The directory whose files are loaded
        :type file_type: str
        :param: file_type: One of sql, email, log, rdx
        :type bucket: str
        :param bucket: GCS bucket name
        :type overwrite: bool
        :param overwrite: Overwrite the target blobs if they exist
        :rtype: None
        :return: Nothing
        :raises: Propagates everything from the underlying package
        """
        formats = FILE_FORMATS.get(file_type, [])
        patts = (
            os.path.join(dirname, '*.{f}*.gz'),
            os.path.join(dirname, '*', '*.{f}*.gz')
        )
        files = []
        for patt in patts:
            for format_ in formats:
                files.extend(glob.glob(patt.format(f=format_)))
        for fname in files:
            self.load_on_file_to_gcs(fname, file_type, bucket, overwrite)
