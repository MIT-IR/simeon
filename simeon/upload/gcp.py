"""
Utilities functions and classes to help with loading data to Google Cloud
"""
import glob
import gzip
import os
from datetime import datetime
from typing import List

from google.cloud import (
    bigquery, exceptions as gcp_exceptions, storage
)

from simeon.upload import utilities as uputils
from simeon.report import utilities as rutils
from simeon.exceptions import LoadJobException
from simeon.upload.utilities import (
    SCHEMA_DIR
)


FILE_FORMATS = {
    'log': ['json'],
    'sql': ['csv', 'txt', 'sql', 'json']
}
MERGE_DDL = """MERGE {first} f USING {second} s
ON f.{column} = s.{column}
WHEN NOT MATCHED THEN
INSERT ROW
"""
DST_DESC = {
    'log': 'Dataset to host the tracking log data from edX courses',
    'sql': (
        'Dataset to host all the tables that are computed from a combination'
        ' of tab-delimited files from edX\'s weekly SQL data dump and '
        'tracking log tables.'
    ),
    'email': (
        'Dataset to host the dimensional details about users\' email'
        ' adresses and email opt-in preferences'
    ),
}


class BigqueryClient(bigquery.Client):
    """
    Subclass bigquery.Client and add convenience methods
    """
    def load_tables_from_dir(
        self, dirname: str, file_type: str, project: str,
        create: bool, append: bool, use_storage: bool=False,
        bucket: str=None, max_bad_rows=0,
        schema_dir=SCHEMA_DIR, format_='json', patch=False,
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
        :type schema_dir: str
        :param schema_dir: Directory where schema files are found
        :type format_: str
        :param format_: File format (json or csv)
        :type patch: bool
        :param patch: Whether or not to patch the description of the table
        :rtype: List[bigquery.LoadJob]
        :returns: List of load jobs
        :raises: Propagates everything from the underlying package
        """
        schema_dir = schema_dir or SCHEMA_DIR
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
                    use_storage, bucket, max_bad_rows, schema_dir, format_,
                    patch
                )
            )
        return jobs

    def load_one_file_to_table(
        self, fname: str, file_type: str, project: str,
        create: bool, append: bool, use_storage: bool=False,
        bucket: str=None, max_bad_rows=0,
        schema_dir=SCHEMA_DIR, format_='json', patch=False,
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
        :type schema_dir: Union[None, str]
        :param schema_dir: Directory where schema files are found
        :type format_: str
        :param format_: File format (json or csv)
        :type patch: bool
        :param patch: Whether or not to patch the description of the table
        :rtype: bigquery.LoadJob
        :returns: The LoadJob object associated with the work being done
        :raises: Propagates everything from the underlying package
        """
        schema_dir = schema_dir or SCHEMA_DIR
        use_storage = use_storage or fname.startswith('gs://')
        if use_storage:
            if bucket is None:
                raise ValueError('use_storage=True requires a bucket name')
            loader = self.load_table_from_uri
        else:
            loader = self.load_table_from_file
        job_prefix = '{t}_data_load_{dt}-'.format(
            t=file_type, dt=datetime.now().strftime('%Y%m%d%H%M%S%f')
        )
        dest = uputils.local_to_bq_table(fname, file_type, project)
        # dataset = self.dataset(dest.rsplit('.', 1)[0])
        dataset = bigquery.Dataset.from_string(dest.rsplit('.', 1)[0])
        dataset.description = DST_DESC.get(file_type)
        self.create_dataset(dataset, exists_ok=True)
        if use_storage:
            if not fname.startswith('gs://'):
                fname = uputils.local_to_gcs_path(fname, file_type, bucket)
        else:
            fname = gzip.open(fname, 'rb')
        config, desc = uputils.make_bq_load_config(
            table=dest, schema_dir=schema_dir,
            append=append, create=create, file_format=format_,
            max_bad_rows=max_bad_rows
        )
        dest = bigquery.Table.from_string(dest)
        dest.description = desc
        if patch:
            # Update the destination table's description.
            # Catch the error raised when the destination table
            # does not exist.
            try:
                self.update_table(dest, ['description'])
            except gcp_exceptions.NotFound:
                pass
        return loader(
            fname, dest, job_config=config, job_id_prefix=job_prefix
        )

    @staticmethod
    def extract_error_messages(errors):
        """
        Return the error messages from given list of error objects (dict)
        """
        messages = []
        if isinstance(errors, dict):
            members = []
            for e in errors.values():
                members += e
        else:
            members = errors
        for err in members:
            msg = err.get('message', '')
            if not msg:
                continue
            src = err.get('source', '')
            if src:
                msg = 'Source: {s} - {m}'.format(s=src, m=msg)
            loc = err.get('location', '')
            if loc:
                msg = '{m} - File: {f}'.format(m=msg, f=loc)
            messages.append(msg)
        return messages

    def merge_to_table(
        self, fname, table, col,
        schema_dir=SCHEMA_DIR, use_storage=False, patch=False,
    ):
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
        :type schema_dir: Union[None, str]
        :param schema_dir: The directory where schema files live
        :type use_storage: bool
        :param use_storage: Whether or not the given path is a GCS URI
        :type patch: bool
        :param patch: Whether or not to patch the description of the table
        :rtype: bigquery.QueryJob
        :returns: The QueryJob object associated with the merge carried out
        :raises: Propagates everything from the underlying package
        """
        schema_dir = schema_dir or SCHEMA_DIR
        if len(table.split('.')) < 3:
            table = '{p}.{t}'.format(p=self.project, t=table)
        dataset = table.rsplit('.', 1)[0]
        bqtable = bigquery.Table.from_string(table)
        temp_table = bigquery.Table.from_string(table + '_temp')
        schema, desc = uputils.get_bq_schema(table, schema_dir=schema_dir)
        bqtable.schema = schema
        bqtable.description = desc
        temp_table.schema = schema
        if patch:
            # Update the destination table's description.
            # Catch the error raised when the destination table
            # does not exist.
            try:
                self.update_table(bqtable, ['description'])
            except gcp_exceptions.NotFound:
                pass
        temp_table.description = desc
        job_prefix = '{t}_data_load_{dt}-'.format(
            t=bqtable.table_id,
            dt=datetime.now().strftime('%Y%m%d%H%M%S%f')
        )
        config, _ = uputils.make_bq_load_config(
            table=table, schema_dir=schema_dir, append=False,
            create=True, file_format='json',
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
        self.delete_table(temp_table, not_found_ok=True)


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
