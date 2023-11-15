"""
Utilities functions and classes to help with loading data to Google Cloud
"""
import glob
import gzip
import hashlib
import os
import threading
import uuid
from datetime import datetime
from typing import List

from google.cloud import bigquery
from google.cloud import exceptions as gcp_exceptions
from google.cloud import storage
from jinja2 import Environment

from simeon.exceptions import LoadJobException
from simeon.report import utilities as rutils
from simeon.upload import utilities as uputils
from simeon.upload.utilities import SCHEMA_DIR, course_to_bq_dataset

FILE_FORMATS = {"log": ["json"], "sql": ["csv", "txt", "sql", "json"]}
MERGE_DDL = """MERGE {{ first }} f USING {{ second }} s
ON f.{{ column }} = s.{{ column }}
WHEN NOT MATCHED THEN
INSERT ROW
{%- if update_cols is defined and update_cols %}
WHEN MATCHED {% if match_equal_columns is defined and match_equal_columns -%}
AND {% for col in match_equal_columns %}
s.{{ col }} = f.{{ col }} {% if not loop.last %} AND {% endif %}
{% endfor %}
{% endif %}
{% if match_unequal_columns is defined and match_unequal_columns -%}
AND {% for col in match_unequal_columns %}
s.{{ col }} <> f.{{ col }} {% if not loop.last %} AND {% endif %}
{% endfor -%}
{% endif -%}
THEN UPDATE SET {% for col in update_cols -%}
    f.{{ col }} = s.{{ col }}{% if not loop.last %}, {% endif %}
{%- endfor %}
{% endif %}
"""
DST_DESC = {
    "log": "Dataset to host the tracking log data from edX courses",
    "sql": (
        "Dataset to host all the tables that are computed from a combination"
        " of tab-delimited files from edX's weekly SQL data dump and "
        "tracking log tables."
    ),
    "email": (
        "Dataset to host the dimensional details about users' email"
        " addresses and email opt-in preferences"
    ),
}


class BigqueryClient(bigquery.Client):
    """
    Subclass bigquery.Client and add convenience methods
    """

    def get_course_tables(self, course_id):
        """
        Get all the tables related to the given course ID

        :type course_id: str
        :param course_id: edX course ID in format ORG/NUMBER/TERM
        :rtype: Dict[str, set]
        :return: A dict with keys as log and sql, and values as table names
        """
        out = {"log": set(), "latest": set()}
        log_dset = course_to_bq_dataset(course_id, "log", self.project)
        latest_dset = course_to_bq_dataset(course_id, "sql", self.project)
        query = f"""select table_schema, table_name
        from {latest_dset}.INFORMATION_SCHEMA.TABLES
        union all
        select table_schema, table_name
        from {log_dset}.INFORMATION_SCHEMA.TABLES"""
        job = self.query(query)
        try:
            for row in job.result():
                ds = row.get("table_schema")
                if ds.endswith("_logs"):
                    out["log"].add(row.get("table_name"))
                else:
                    out["latest"].add(row.get("table_name"))
        except Exception:
            # If the dataset does not exist, then we end up here.
            # So, do nothing. The returned value will be a dict with
            # no table names.
            pass
        return out

    def has_latest_table(self, course_id, table):
        """
        Check if the given table name exists in the _latest dataset
        of the given course ID

        :type course_id: str
        :param course_id: edX course ID in format ORG/NUMBER/TERM
        :type table: str
        :param table: Name of the table being looked up
        :rtype: bool
        :return: True if the table is currently in BigQuery
        """
        latest_dset = course_to_bq_dataset(course_id, "sql", self.project)
        query = f"""select table_schema, table_name
        from {latest_dset}.INFORMATION_SCHEMA.TABLES
        where table_name = '{table}'"""
        # We could have used count(*) for the query, but if we ever switch from
        # returning a boolean to returning the actual matches, we would never need
        # to change the query.
        job = self.query(query)
        count = 0
        try:
            for _ in job.result():
                count += 1
        except Exception:
            pass
        return count != 0

    def has_log_table(self, course_id, table):
        """
        Check if the given table name exists in the _logs dataset of
        the given course ID

        :type course_id: str
        :param course_id: edX course ID in format ORG/NUMBER/TERM
        :type table: str
        :param table: Name of the table being looked up
        :rtype: bool
        :return: True if the table is currently in BigQuery
        """
        log_dset = course_to_bq_dataset(course_id, "log", self.project)
        query = f"""select table_schema, table_name
        from {log_dset}.INFORMATION_SCHEMA.TABLES
        where table_name like '%{table}%'"""
        # We could have used count(*) for the query, but if we ever switch from
        # returning a boolean to returning the actual matches, we would never need
        # to change the query.
        job = self.query(query)
        count = 0
        # job.result() does not return an object with __len__ defined.
        # So, it would make sense to iterate over the returned records
        # and count stuff.
        try:
            for _ in job.result():
                count += 1
        except Exception:
            pass
        return count != 0

    def make_template(self, query):
        """
        Create a Template object whose environment includes some of the
        client's methods as filters

        :type query: str
        :param query: SQL query to use with the template being generated
        :rtype: jinja2.Template
        :return: Jinja2 Template object with the passed query
        """
        jinja_env = Environment()
        jinja_env.filters["get_course_tables"] = self.get_course_tables
        jinja_env.filters["has_latest_table"] = self.has_latest_table
        jinja_env.filters["has_log_table"] = self.has_log_table
        return jinja_env.from_string(query)

    def load_tables_from_dir(
        self,
        dirname: str,
        file_type: str,
        project: str,
        create: bool,
        append: bool,
        use_storage: bool = False,
        bucket: str = None,
        max_bad_rows=0,
        schema_dir=SCHEMA_DIR,
        format_="json",
        patch=False,
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
            os.path.join(dirname, "*.{f}*.gz"),
            os.path.join(dirname, "*", "*.{f}*.gz"),
        )
        files = []
        for patt in patts:
            for format_ in formats:
                files.extend(glob.glob(patt.format(f=format_)))
        jobs = []
        for file_ in files:
            jobs.append(
                self.load_one_file_to_table(
                    file_,
                    file_type,
                    project,
                    create,
                    append,
                    use_storage,
                    bucket,
                    max_bad_rows,
                    schema_dir,
                    format_,
                    patch,
                )
            )
        return jobs

    def load_one_file_to_table(
        self,
        fname: str,
        file_type: str,
        project: str,
        create: bool,
        append: bool,
        use_storage: bool = False,
        bucket: str = None,
        max_bad_rows=0,
        schema_dir=SCHEMA_DIR,
        format_="json",
        patch=False,
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
        use_storage = use_storage or fname.startswith("gs://")
        if use_storage:
            if bucket is None:
                raise ValueError("use_storage=True requires a bucket name")
            loader = self.load_table_from_uri
        else:
            loader = self.load_table_from_file
        job_prefix = "simeon_{t}_data_load_{dt}-".format(
            t=file_type, dt=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        dest = uputils.local_to_bq_table(fname, file_type, project)
        # dataset = self.dataset(dest.rsplit('.', 1)[0])
        dataset = bigquery.Dataset.from_string(dest.rsplit(".", 1)[0])
        dataset.description = DST_DESC.get(file_type)
        self.create_dataset(dataset, exists_ok=True)
        if use_storage:
            if not fname.startswith("gs://"):
                fname = uputils.local_to_gcs_path(fname, file_type, bucket)
        else:
            fname = gzip.open(fname, "rb")
        config, desc = uputils.make_bq_load_config(
            table=dest,
            schema_dir=schema_dir,
            append=append,
            create=create,
            file_format=format_,
            max_bad_rows=max_bad_rows,
        )
        dest = bigquery.Table.from_string(dest)
        dest.description = desc
        if patch:
            # Update the destination table's description.
            # Catch the error raised when the destination table
            # does not exist.
            try:
                self.update_table(dest, ["description"])
            except gcp_exceptions.NotFound:
                pass
        return loader(fname, dest, job_config=config, job_id_prefix=job_prefix)

    @staticmethod
    def get_not_found_object(message):
        """
        If the given message contains the keywords 'Not found', then
        try and determine the name and type of the object that is not found.
        """
        out = dict.fromkeys(("missing_object_type", "missing_object_name"))
        if "Not found" not in (message or ""):
            return out
        pieces = iter(message.split("Not found: ")[-1].split()[:2])
        out["missing_object_type"] = next(pieces, None)
        out["missing_object_name"] = next(pieces, None)
        return out

    @staticmethod
    def export_compiled_query(query, table, target_directory):
        """
        Export a query string to the target directory

        :type query: str
        :param query: Compiled SQL query that is sent to BigQuery
        :type table: str
        :param table: Name of the table that is generated by the given query
        :type target_directory: str
        :param target_directory: The directory under which the compiled SQL query is stored
        :rtype: None
        :return: Stores SQL query under the given target directory
        """
        target_directory = target_directory or "target"
        # Generate a directory inside the target directory where to store
        # the queries that belong to the running OS process.
        # This makes it so that no two OS processes/threads will clobber
        # each other's query exports.
        # This function has no control over whether or not the given table name is
        # a fully qualified name. As a result, it can't count on project and dataset
        # names to distinguish between tables of the same name but from different courses.
        # LOGIC: Use an MD5 hasher and update it with the current process ID and the python
        # specific thread ID. Once that's done, we take the hex digest as a subfolder name.
        hasher = hashlib.md5()
        hasher.update(str(os.getpid()).encode())
        hasher.update(str(threading.get_ident()).encode())
        unique_id = hasher.hexdigest()
        full_target_directory = os.path.join(
            os.path.expanduser(target_directory), "compiled", unique_id
        )
        filename = table + ".sql"
        os.makedirs(full_target_directory, exist_ok=True)
        with open(os.path.join(full_target_directory, filename), "w") as fh:
            fh.write(query)

    @staticmethod
    def extract_error_messages(errors):
        """
        Return the error messages from given list of error objects (dict)
        """
        messages = {}
        errors = errors or []
        members = []
        if isinstance(errors, dict):
            for e in errors.values():
                members += e
        else:
            members = errors
        for err in members:
            msg = err.get("message", "")
            if not msg:
                continue
            context = dict()
            src = err.get("source", "")
            if src:
                context["source"] = src
                msg = "Source: {s} - {m}".format(s=src, m=msg)
            loc = err.get("location", "")
            if loc:
                context["file"] = loc
                msg = "{m} - File: {f}".format(m=msg, f=loc)
            context.update(BigqueryClient.get_not_found_object(msg))
            messages[msg] = context
        return messages

    def merge_to_table(
        self,
        fname,
        table,
        col,
        schema_dir=SCHEMA_DIR,
        use_storage=False,
        patch=False,
        match_equal_columns=None,
        match_unequal_columns=None,
        target_directory="target",
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
        :type match_equal_columns: Union[List[str], None, Tuple[str]]
        :param match_equal_columns: List of column names for which to set equality (=) if WHEN MATCH is met during the merge.
        :type match_unequal_columns: Union[List[str], None, Tuple[str]]
        :param match_unequal_columns: List of column names for which to set inequality (<>) if WHEN MATCH is met during the merge.
        :type target_directory: str
        :param target_directory: Target directory where to store SQL queries
        :rtype: bigquery.QueryJob
        :returns: The QueryJob object associated with the merge carried out
        :raises: Propagates everything from the underlying package
        """
        schema_dir = schema_dir or SCHEMA_DIR
        if len(table.split(".")) < 3:
            table = "{p}.{t}".format(p=self.project, t=table)
        dataset = table.rsplit(".", 1)[0]
        bqtable = bigquery.Table.from_string(table)
        temp_table_name = "{t}_{d}_{u}_temp".format(
            t=table,
            d=datetime.now().strftime("%Y%m%d%H%M%S%f"),
            u=uuid.uuid4().hex.replace("-", "").replace(".", ""),
        )
        temp_table = bigquery.Table.from_string(temp_table_name)
        schema, desc = uputils.get_bq_schema(table, schema_dir=schema_dir)
        bqtable.schema = schema
        bqtable.description = desc
        temp_table.schema = schema
        if patch:
            # Update the destination table's description.
            # Catch the error raised when the destination table
            # does not exist.
            try:
                self.update_table(bqtable, ["schema", "description"])
            except gcp_exceptions.NotFound:
                pass
        temp_table.description = desc
        job_prefix = "simeon_{t}_data_load_{dt}-".format(
            t=bqtable.table_id, dt=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        config, _ = uputils.make_bq_load_config(
            table=table,
            schema_dir=schema_dir,
            append=False,
            create=True,
            file_format="json",
        )
        self.create_dataset(dataset, exists_ok=True)
        for tbl in (bqtable, temp_table):
            self.create_table(tbl, exists_ok=True)
        if use_storage:
            loader = self.load_table_from_uri
        else:
            loader = self.load_table_from_file
            fname = gzip.open(fname, "rb")
        job = loader(fname, temp_table, job_config=config, job_id_prefix=job_prefix)
        rutils.wait_for_bq_jobs([job])
        if job.errors:
            self.delete_table(temp_table, not_found_ok=True)
            # Extract the BigQuery errors and their context info
            errors = self.extract_error_messages(job.errors)
            context = dict()
            for v in errors.values():
                context.update(v)
            # Raise an exception with some contextual information
            raise LoadJobException("\n".join(errors), context_dict=context)
        query_template = self.make_template(MERGE_DDL)
        update_cols = [f.name for f in bqtable.schema if f.name.lower() != col.lower()]
        query = query_template.render(
            first=table,
            second=temp_table_name,
            column=col,
            update_cols=update_cols,
            match_equal_columns=match_equal_columns,
            match_unequal_columns=match_unequal_columns,
        )
        query_job = self.query(query)
        # Export the compiled query in the given target directory under
        # a folder called compiled.
        self.export_compiled_query(
            query=query, table=table, target_directory=target_directory
        )
        rutils.wait_for_bq_jobs([query_job])
        if query_job.errors:
            self.delete_table(temp_table, not_found_ok=True)
            errors = self.extract_error_messages(query_job.errors)
            context = dict()
            for v in errors.values():
                context.update(v)
            raise LoadJobException("\n".join(errors), context_dict=context)
        self.delete_table(temp_table, not_found_ok=True)


class GCSClient(storage.Client):
    """
    Make a client to load data files to GCS
    """

    def load_one_file_to_gcs(self, fname: str, file_type: str, bucket: str):
        """
        Load the given file to GCS

        :type fname: str
        :param fname: The local file to load to GCS
        :type file_type: str
        :param: file_type: One of sql, email, log, rdx
        :type bucket: str
        :param bucket: GCS bucket name
        :rtype: None
        :returns: Nothing, but should load the given file to GCS
        :raises: Propagates everything from the underlying package
        """
        dest = storage.Blob.from_string(
            uputils.local_to_gcs_path(fname, file_type, bucket), client=self
        )
        if "cold" in file_type.lower():
            dest.storage_class = storage.constants.COLDLINE_STORAGE_CLASS
        dest.upload_from_filename(fname, timeout=20 * 60)

    def load_dir(self, dirname: str, file_type: str, bucket: str):
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
            os.path.join(dirname, "*.{f}*.gz"),
            os.path.join(dirname, "*", "*.{f}*.gz"),
        )
        files = []
        for patt in patts:
            for format_ in formats:
                files.extend(glob.glob(patt.format(f=format_)))
        for fname in files:
            self.load_one_file_to_gcs(fname, file_type, bucket)
