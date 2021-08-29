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
    'cold': 'COLD',
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
        raise ValueError(
            'file_type is not one of these: {ft}'.format(
                ft=', '.join(SEGMENTS)
            )
        )
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
    :param file_type: One of sql, log, email, rdx, cold
    :type bucket: str
    :param bucket: A GCS bucket name
    :rtype: str
    :return: A nicely formatted GCS path
    """
    segment = SEGMENTS.get(file_type)
    if not segment:
        msg = 'file_type is not one of these: {s}'
        raise ValueError(msg.format(s=', '.join(SEGMENTS)))
    if not bucket.startswith('gs://'):
        bucket = 'gs://{b}'.format(b=bucket)
    dname, bname = os.path.split(os.path.abspath(os.path.expanduser(fname)))
    if segment == 'COLD':
        gcs_file = bname
    else:
        gcs_file = '{d}/{f}'.format(
            d=os.path.basename(dname).replace('.', '_').replace('-', '_'),
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
        msg = 'file_type is not one of these: {s}'
        raise ValueError(msg.format(s=', '.join(SEGMENTS)))
    if file_type in ('sql',):
        suffix = '_latest'
    elif file_type == 'log':
        suffix = '_logs'
    else:
        suffix = ''
    dataset = '{d}{s}'.format(
        d=course_id.replace('/', '__').replace('.', '_').replace('-', '_'),
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
    if file_type in ('sql',):
        table = bname.split('.', 1)[0].replace('-', '_')
        suffix = '_latest'
    elif file_type in ('log',):
        suffix = '_logs'
        table = 'tracklog_' + ''.join(re.findall(r'\d+', bname))
    else:
        table = bname.split('.', 1)[0].replace('-', '_')
        suffix = ''
    dataset = os.path.basename(dname).replace('.', '_').replace('-', '_')
    if not table.replace('tracklog_', '') or not dataset:
        msg = (
            'A BigQuery table name for file type {t} '
            'could not be constructed with file name {f}. '
            'Please make sure that you are using the right file type.'
        )
        raise BigQueryNameException(
            msg.format(f=fname, t=file_type)
        )
    return '{p}.{d}{s}.{t}'.format(
        d=dataset, s=suffix, p=project, t=table
    )


def dict_to_schema_field(schema_dict: dict):
    """
    Make a SchemaField from a schema directory

    :type schema_dict: dict
    :param schema_dict: One of the objects in the schema JSON file
    :rtype: bigquery.SchemaField
    :returns: A SchemaField matching the given dictionary's name, type, etc.
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
    :rtype: Tuple[List[bigquery.SchemaField], str]
    :return: A 2-tuple with list of bigquery.SchemaField objects and
        a description text for the target table
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
    if not out:
        msg = (
            'The schema file {f!r} does not contain an object '
            'with a name matching the given table {t}'
        )
        raise MissingSchemaException(msg.format(f=schema_file, t=table))
    return out, schema.get('description')


def make_bq_load_config(
    table: str, schema_dir=SCHEMA_DIR, append: bool=False,
    create: bool=True, file_format: str='json', delim=',', max_bad_rows=0,
):
    """
    Make a bigquery.LoadJobConfig object and description of a table

    :type table: str
    :param table: Fully qualified table name
    :type schema_dir: str
    :param schema_dir: The directory where schema files live
    :type append: bool
    :param append: Whether to append the loaded to the table
    :type create: bool
    :param create: Whether to create the target table if it does not exist
    :type file_format: str
    :param file_format: One of sql, json, csv, txt
    :type delim: str
    :param delim: The delimiter of the file being loaded
    :rtype: Tuple[bigquery.LoadJobConfig, str]
    :return: A 2-tuple with a bigquery.LoadJobConfig object and
        a description text for the destination table
    """
    schema_dir = schema_dir or SCHEMA_DIR
    schema, desc = get_bq_schema(table, schema_dir=schema_dir)
    if 'json' in file_format.lower():
        format_ = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        delim = None
        skips = None
    else:
        format_ = bigquery.SourceFormat.CSV
        skips = 1
    if create:
        create = bigquery.CreateDisposition.CREATE_IF_NEEDED
    else:
        create = bigquery.CreateDisposition.CREATE_NEVER
    if append:
        append = bigquery.WriteDisposition.WRITE_APPEND
    else:
        append = bigquery.WriteDisposition.WRITE_TRUNCATE
    if 'json' in file_format.lower():
        config = bigquery.LoadJobConfig(
            schema=schema, source_format=format_,
            create_disposition=create, write_disposition=append,
            max_bad_records=max_bad_rows, ignore_unknown_values=True,
            destination_table_description=desc,
        )
        return config, desc
    config = bigquery.LoadJobConfig(
        schema=schema, source_format=format_,
        create_disposition=create, write_disposition=append,
        field_delimiter=delim, skip_leading_rows=skips,
        max_bad_records=max_bad_rows, ignore_unknown_values=True,
        allow_quoted_newlines=True, destination_table_description=desc,
    )
    return config, desc


def make_bq_query_config(append: bool=False, plain=True, table=None):
    """
    Make a bigquery.QueryJobConfig object to tie to a query to be sent
    to BigQuery for secondary table generation

    :type append: bool
    :param append: Whether to append the loaded to the table
    :type plain: bool
    :param plain: Make an empty QueryJobConfig object
    :type table: Union[None, str]
    :param table: Fully qualified name of a destination table
    :rtype: bigquery.QueryJobConfig
    :returns: Make a bigquery.QueryJobConfig object
    """
    if plain:
        return bigquery.job.QueryJobConfig()
    if append:
        append = bigquery.WriteDisposition.WRITE_APPEND
    else:
        append = bigquery.WriteDisposition.WRITE_TRUNCATE
    config = bigquery.job.QueryJobConfig()
    if table is not None:
        config.destination = bigquery.Table.from_string(table)
    config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    config.write_disposition = append
    config.allow_large_results = True
    config.use_legacy_sql = False
    return config


def sqlify_bq_field(field, named=True):
    """
    Convert a bigquery.SchemaField object into a DDL
    column definition.

    :type field: bigquery.SchemaField
    :param field: A SchemaField object to convert to a DDL statement
    :type named: bool
    :param named: Whether the returned statement start with the field name
    :rtype: str
    :returns: A SQL column's DDL statement
    """
    nullability = '' if field.is_nullable else 'NOT NULL'
    if 'INTEGER' in field.field_type:
        type_ = 'INT64'
    elif 'FLOAT' in field.field_type:
        type_ = 'FLOAT64'
    else:
        type_ = field.field_type
    if type_ != 'RECORD':
        if field.mode != 'REPEATED':
            return '{n} {t} {m} OPTIONS(description="""{d}""")'.format(
                n=field.name if named else '',
                t=type_,
                m=nullability,
                d=field.description or '',
            )
        else:
            return '{n} ARRAY<{t}> {m} OPTIONS(description="""{d}""")'.format(
                n=field.name if named else '',
                t=type_,
                m=nullability,
                d=field.description or '',
            )
    if type_ == 'RECORD' and field.mode != 'REPEATED':
        return '{n} STRUCT<{t}> {m} OPTIONS(description="""{d}"""'.format(
            t=',\n\t'.join(sqlify_bq_field(f) for f in field.fields),
            n=field.name if named else '',
            m=nullability,
            d=field.description or '',
        )
    field = bigquery.SchemaField(
        name=field.name, field_type=field.field_type,
        mode='NULLABLE' if field.is_nullable else 'REQUIRED',
        description=field.description, fields=field.fields,
    )
    return '{n} ARRAY<{t}> {m} OPTIONS(description="""{d}""")'.format(
        t=sqlify_bq_field(field, False).lstrip(' '),
        n=field.name,
        m=nullability,
        d=field.description or '',
    )
