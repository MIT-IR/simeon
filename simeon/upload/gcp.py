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
from simeon.report import utilities as rutils
from simeon.exceptions import LoadJobException


FILE_FORMATS = {
    'log': ['json'],
    'sql': ['csv', 'txt', 'sql', 'json']
}
MERGE_DDL = """MERGE {first} f USING {second} s
ON f.{column} = s.{column}
WHEN NOT MATCHED THEN
INSERT ROW
"""


class BigqueryClient(bigquery.Client):
    """
    Subclass bigquery.Client and add convenience methods
    """
    def load_tables_from_dir(
        self, dirname: str, file_type: str, project: str,
        create: bool, append: bool, use_storage: bool=False,
        bucket: str=None, max_bad_rows=0,
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
        :type bucket: str
        :param bucket: GCS bucket name to use
        :type max_bad_rows: int
        :param max_bad_rows: Max number of bad rows allowed during loading
        :rtype: List[bigquery.LoadJob]
        :returns: List of load jobs
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
        jobs = []
        for file_ in files:
            jobs.append(
                self.load_one_file_to_table(
                    file_, file_type, project, create, append,
                    use_storage, bucket, max_bad_rows
                )
            )
        return jobs

    def load_one_file_to_table(
        self, fname: str, file_type: str, project: str,
        create: bool, append: bool, use_storage: bool=False,
        bucket: str=None, max_bad_rows=0,
    ):
        """
        Load the given file to a target BigQuery table

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
        :type max_bad_rows: int
        :param max_bad_rows: Max number of bad rows allowed during loading
        :rtype: bigquery.LoadJob
        :returns: The LoadJob object associated with the work being done
        :raises: Propagates everything from the underlying package
        """
        use_storage = use_storage or fname.startswith('gs://')
        if use_storage:
            if bucket is None:
                raise ValueError('use_storage=True requires a bucket name')
            loader = self.load_table_from_uri
        else:
            loader = self.load_table_from_file
        # format_ = 'json' if file_type == 'log' else 'csv'
        # We'll force all formats to JSON
        format_ = 'json'
        job_prefix = '{t}_data_load_{dt}-'.format(
            t=file_type, dt=datetime.now().strftime('%Y%m%d%H%M%S%f')
        )
        dest = uputils.local_to_bq_table(fname, file_type, project)
        dataset = dest.rsplit('.', 1)[0]
        self.create_dataset(dataset, exists_ok=True)
        if use_storage:
            if not fname.startswith('gs://'):
                fname = uputils.local_to_gcs_path(fname, file_type, bucket)
        else:
            fname = gzip.open(fname, 'rb')
        config = uputils.make_bq_load_config(
            dest, append, create, format_, max_bad_rows=max_bad_rows
        )
        return loader(
            fname, dest, job_config=config, job_id_prefix=job_prefix
        )

    @staticmethod
    def extract_error_messages(errors):
        """
        Return the error messages from given list of error objects (dict)
        """
        messages = []
        for err in errors:
            msg = err.get('message', '')
            if not msg:
                continue
            loc = err.get('location', '')
            if loc:
                msg = '{m} - File: {f}'.format(m=msg, f=loc)
            messages.append(msg)
        return messages

    def merge_to_table(self, fname, table, col, use_storage=False):
        """
        Merge the given file to the target table name.
        If the latter does not exist, create it first.
        This process waits for all the jobs needed

        :type fname: str
        :param fname: A local file name or a GCS URI
        :type table: str
        :param table: Fully qualified BigQuery table name
        :type col: str
        :param col: Column by which to merge
        :type use_storage: bool
        :param use_storage: Whether or not the given path is a GCS URI
        :rtype: bigquery.QueryJob
        :returns: The QueryJob object associated with the merge carried out
        :raises: Propagates everything from the underlying package
        """
        if len(table.split('.')) < 3:
            table = '{p}.{t}'.format(p=self.project, t=table)
        dataset = table.rsplit('.', 1)[0]
        bqtable = bigquery.Table.from_string(table)
        temp_table = bigquery.Table.from_string(table + '_temp')
        schema = uputils.get_bq_schema(table)
        bqtable.schema = schema
        temp_table.schema = schema
        job_prefix = '{t}_data_load_{dt}-'.format(
            t=bqtable.table_id,
            dt=datetime.now().strftime('%Y%m%d%H%M%S%f')
        )
        config = config = uputils.make_bq_load_config(
            table, False, True, 'json'
        )
        self.create_dataset(dataset, exists_ok=True)
        for tbl in (bqtable, temp_table):
            self.create_table(tbl, exists_ok=True)
        if use_storage:
            loader = self.load_table_from_uri
        else:
            loader = self.load_table_from_file
            fname = gzip.open(fname, 'rb')
        job = loader(
            fname, temp_table, job_config=config, job_id_prefix=job_prefix
        )
        rutils.wait_for_bq_job_ids([job.job_id], self)
        if job.errors:
            msg = 'Merge job failed with: {e}'
            raise LoadJobException(msg.format(
                e='\n'.join(self.extract_error_messages(job.errors))
            ))
        query = MERGE_DDL.format(
            first=table, second=table + '_temp', column=col,
        )
        qjob = self.query(query)
        rutils.wait_for_bq_job_ids([qjob.job_id], self)
        if qjob.errors:
            msg = 'Merge job failed with: {e}'
            raise LoadJobException(msg.format(
                e='\n'.join(self.extract_error_messages(job.errors))
            ))
        self.delete_table(temp_table)


class GCSClient(storage.Client):
    """
    Make a client to load data files to GCS
    """
    def load_one_file_to_gcs(
        self, fname: str, file_type: str, bucket: str
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
        :returns: Nothing, but should load the given file to GCS
        :raises: Propagates everything from the underlying package
        """
        dest = storage.Blob.from_string(
            uputils.local_to_gcs_path(fname, file_type, bucket),
            client=self
        )
        if 'cold' in file_type.lower():
            dest.storage_class = storage.constants.COLDLINE_STORAGE_CLASS
        dest.upload_from_filename(fname, timeout=20 * 60)

    def load_dir(
        self, dirname: str, file_type: str, bucket: str
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
        :rtype: None
        :returns: Nothing, but should load file(s) in dirname to GCS
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
            self.load_one_file_to_gcs(fname, file_type, bucket)
