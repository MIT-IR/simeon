"""
Utility functions and classes associated with uploading data to GCP, so far.
"""
import json
import os
import re

from google.cloud import bigquery
from google.cloud import storage

from simeon.exceptions import (
    BigQueryNameException, MissingSchemaException
)


SEGMENTS = {
    'log': 'TRACKING-LOGS',
    'email': 'EMAIL',
    'sql': 'SQL',
    'rdx': 'RDX',
}
SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'schemas'
)
SCHEMAS = {}


def course_to_gcs_folder(course_id: str, file_type: str, bucket: str) -> str:
    """
    Use the given course ID to make a Google Cloud Storage path

    :type course_id: str
    :param course_id: edX course ID to format into a GCS path
    :type file_type: str
    :param file_type: One of sql, log, email, rdx
    :type bucket: str
    :param bucket: A GCS bucket name
    :rtype: str
    :return: A nicely formatted GCS path
    """
    segment = SEGMENTS.get(file_type)
    if not segment:
        raise ValueError('file_type is not one of these: sql, email, log')
    if not bucket.startswith('gs://'):
        bucket = 'gs://{b}'.format(b=bucket)
    dirname = course_id.replace('/', '__').replace('.', '_')
    return '{b}/{s}/{d}'.format(b=bucket, s=segment, d=dirname)


def local_to_gcs_path(fname: str, file_type: str, bucket: str) -> str:
    """
    Convert the local file name into a GCS path

    :type fname: str
    :param fname: A local file name
    :type file_type: str
    :param file_type: One of sql, log, email, rdx
    :type bucket: str
    :param bucket: A GCS bucket name
    :rtype: str
    :return: A nicely formatted GCS path
    """
    segment = SEGMENTS.get(file_type)
    if not segment:
        raise ValueError(
            'file_type is not one of these: {s}'.format(s=', '.join(SEGMENTS))
        )
    if not bucket.startswith('gs://'):
        bucket = 'gs://{b}'.format(b=bucket)
    dname, bname = os.path.split(os.path.abspath(os.path.expanduser(fname)))
    gcs_file = '{d}/{f}'.format(
        d=os.path.basename(dname).replace('.', '_'),
        f=bname
    )
    return '{b}/{s}/{f}'.format(b=bucket, s=segment, f=gcs_file)


def course_to_bq_dataset(course_id: str, file_type: str, project: str) -> str:
    """
    Make a fully qualified BigQuery dataset name with the given info

    :type course_id: str
    :param course_id: edX course ID to format into a GCS path
    :type file_type: str
    :param file_type: One of sql, log, email, rdx
    :type project: str
    :param project: A GCP project ID
    :rtype: str
    :return: BigQuery dataset name with components separated by dots
    """
    if file_type not in SEGMENTS:
        raise ValueError(
            'file_type is not one of these: {s}'.format(
                s=', '.join(s=SEGMENTS)
            )
        )
    suffix = 'logs'
    if file_type in ('sql', 'email'):
        suffix = 'latest'
    dataset = '{d}_{s}'.format(
        d=course_id.replace('/', '__').replace('.', '_'),
        s=suffix
    )
    return '{p}.{d}'.format(p=project, d=dataset)


def local_to_bq_table(fname: str, file_type: str, project: str) -> str:
    """
    Use the given local file to make a fully qualified BigQuery table name

    :type fname: str
    :param fname: A local file name
    :type file_type: str
    :param file_type: One of sql, log, email, rdx
    :type project: str
    :param project: A GCP project ID
    :rtype: str
    :return: BigQuery dataset name with components separated by dots
    """
    if file_type not in SEGMENTS:
        raise ValueError(
            'file_type is not one of these: {s}'.format(
                s=', '.join(SEGMENTS)
            )
        )
    dname, bname = os.path.split(os.path.abspath(os.path.expanduser(fname)))
    if file_type in ('sql', 'email'):
        table = bname.split('.', 1)[0].replace('-', '_')
        suffix = 'latest'
    else:
        suffix = 'logs'
        table = 'tracklog_' + ''.join(re.findall(r'\d+', bname))
    dataset = os.path.basename(dname).replace('.', '_')
    if not table.replace('tracklog_', '') or not dataset:
        raise BigQueryNameException(
            'A BigQuery table name could not be constructed with {f}'.format(
                f=fname
            )
        )
    return '{p}.{d}_{s}.{t}'.format(
        d=dataset, s=suffix, p=project, t=table
    )


def dict_to_schema_field(schema_dict: dict):
    """
    Make a SchemaField
    """
    if schema_dict.get('field_type') != 'RECORD':
        return bigquery.SchemaField(**schema_dict)
    fields = []
    for subfield in schema_dict.get('fields', []):
        fields.append(dict_to_schema_field(subfield))
    schema_dict['fields'] = fields
    field = bigquery.SchemaField(**schema_dict)
    return field


def get_bq_schema(table: str, schema_dir: str=SCHEMA_DIR):
    """
    Given a bare table name (without leading project or dataset),
    make a list of bigquery.SchemaField objects to act as the table's schema.

    :type table: str
    :param table: A BigQuery (bare) table name
    :type schema_dir: str
    :param schema_dir: Directory where schema JSON file is looked up
    :rtype: List[bigquery.SchemaField]
    :return: A list of bigquery.SchemaField objects
    """
    bname = table.split('.')[-1]
    if all(k in bname for k in ('track', 'log')):
        bname = 'tracking_log'
    schema_file = os.path.join(schema_dir, 'schema_{t}.json'.format(t=bname))
    if not os.path.exists(schema_file):
        raise MissingSchemaException(
            'No JSON schema file found for {t} in directory {d}'.format(
                t=table, d=schema_dir
            )
        )
    out = []
    with open(schema_file) as jf:
        schema = json.load(jf)
        for field in schema.get(bname, []):
            out.append(dict_to_schema_field(field))
    return out


def make_bq_config(
    table: str, append: bool=False,
    create: bool=True, file_format: str='json'
):
    """
    Make a bigquery.LoadConfig object
    """
    schema = get_bq_schema(table)
    if 'json' in file_format.lower():
        file_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    else:
        file_format = bigquery.SourceFormat.CSV
    if create:
        create = bigquery.CreateDisposition.CREATE_IF_NEEDED
    else:
        create = bigquery.CreateDisposition.CREATE_NEVER
    if append:
        append = bigquery.WriteDisposition.WRITE_APPEND
    else:
        append = bigquery.WriteDisposition.WRITE_TRUNCATE
    return bigquery.LoadJobConfig(
        schema=schema, source_format=file_format,
        create_disposition=create, write_disposition=append,

    )
