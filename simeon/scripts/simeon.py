"""
simeon is a command line tool that helps with processing edx data
"""
import functools
import glob
import multiprocessing as mp
import os
import signal
import sys
import traceback
from argparse import (
    ArgumentParser, FileType, RawDescriptionHelpFormatter
)

from simeon.download import (
    aws, emails, logs, sqls, utilities as downutils
)
from simeon.exceptions import (
    AWSException, EarlyExitError
)
from simeon.report import (
    make_sql_tables, make_table_from_sql, wait_for_bq_jobs
)
from simeon.scripts import utilities as cli_utils
from simeon.upload import gcp


logger = None


def bail_out(sig, frame):
    """
    Exit somewhat cleanly from a signal
    """
    if logger:
        logger.warning('The process is being interrupted by a signal.')
    if logger:
        logger.warning('Waiting for child processes...')
    children = mp.active_children()
    for child in children:
        try:
            # os.kill(child.pid, sig)
            # os.waitpid(child.pid, 0)
            child.terminate()
        except:
            continue
    if logger:
        logger.warning(
            'Incomplete splitting will leave generated files in an incomplete'
            ' state. Please make sure to clean up manually.'
        )
        logger.warning(
            'You may also have to hit CTRL+C again to fully exit, '
            'if the first one does not fully terminate the program.'
        )
        logger.warning('Exiting...')
    sys.exit(1)


def list_files(parsed_args):
    """
    Using the Namespace object generated by argparse, list the files
    that match the given criteria
    """
    start_year = int(parsed_args.begin_date[:4])
    end_year = int(parsed_args.end_date[:4])
    info = aws.BUCKETS.get(parsed_args.file_type)
    bucket = aws.make_s3_bucket(info['Bucket'])
    blobs = []
    for year in range(start_year, end_year + 1):
        prefix = info['Prefix'].format(
            site=parsed_args.site, year=year,
            date=parsed_args.begin_date, org=parsed_args.org,
            request=parsed_args.request_id or '',
        )
        blobs += aws.S3Blob.from_prefix(
            bucket=bucket, prefix=prefix
        )
    for blob in blobs:
        fdate = aws.get_file_date(blob.name)
        if parsed_args.begin_date <= fdate <= parsed_args.end_date:
            if parsed_args.json:
                print(blob.to_json())
            else:
                print(blob)


def split_log_files(parsed_args):
    """
    Using the Namespace object generated by argparse, parse the given
    tracking log files and put them in the provider destination directory
    """
    files = parsed_args.downloaded_files
    success = 0
    if len(files) == 1 or parsed_args.dynamic_date:
        for fname in files:
            msg = 'Splitting {f}'.format(f=fname)
            parsed_args.logger.info(msg)
            rc = logs.split_tracking_log(
                filename=fname,
                ddir=parsed_args.destination,
                dynamic_date=parsed_args.dynamic_date,
                courses=parsed_args.courses,
            )
            if not rc:
                errmsg = (
                    'No files were extracted while splitting the tracking '
                    'log file {f!r} with the given criteria.'
                )
                parsed_args.logger.warning(errmsg.format(f=fname))
            parsed_args.logger.info('Done splitting {f}'.format(f=fname))
            success += rc
    else:
        success = logs.batch_split_tracking_logs(
            filenames=files,
            ddir=parsed_args.destination,
            dynamic_date=False,
            courses=parsed_args.courses,
            verbose=parsed_args.verbose,
            logger=parsed_args.logger,
            size=parsed_args.jobs,
        )
    sys.exit(0 if success else 1)


def split_sql_files(parsed_args):
    """
    Split the SQL data archive into separate folders.
    """
    failed = False
    msg = '{w} file name {f}'
    for fname in parsed_args.downloaded_files:
        parsed_args.logger.info(
            msg.format(f=fname, w='Splitting')
        )
        try:
            to_decrypt = sqls.process_sql_archive(
                archive=fname, ddir=parsed_args.destination,
                include_edge=parsed_args.include_edge,
                courses=parsed_args.courses,
            )
            if not to_decrypt:
                errmsg = (
                    'No files extracted while splitting the '
                    'contents of {f!r} with the given criteria. '
                    'Moving on'
                )
                parsed_args.logger.warning(errmsg.format(f=fname))
                parsed_args.logger.warning(msg.format(
                    f=fname, w='Done splitting'
                ))
                continue
            parsed_args.logger.info(
                msg.format(f=fname, w='Done splitting')
            )
            if parsed_args.no_decryption:
                continue
            parsed_args.logger.info(
                msg.format(f=fname, w='Decrypting the contents in')
            )
            sqls.batch_decrypt_files(
                all_files=to_decrypt, size=100,
                verbose=parsed_args.verbose, logger=parsed_args.logger,
                timeout=parsed_args.decryption_timeout,
                keepfiles=parsed_args.keep_encrypted
            )
            parsed_args.logger.info(
                msg.format(f=fname, w='Done decrypting the contents in')
            )
            dirnames = set(
                os.path.dirname(f) for f in to_decrypt if 'ora/' not in f
            )
            parsed_args.logger.info('Making reports from course SQL files')
            for folder in dirnames:
                make_sql_tables(folder, parsed_args.verbose, parsed_args.logger)
            parsed_args.logger.info('Course reports generated')
        except:
            _, excp, tb = sys.exc_info()
            if isinstance(excp, SystemExit):
                raise excp
            msg = 'Failed to split and decrypt {f}: {e}'
            if parsed_args.debug:
                traces = ['{e}'.format(e=excp)]
                traces += map(str.strip, traceback.format_tb(tb))
                msg = msg.format(f=fname, e='\n'.join(traces))
            else:
                msg = msg.format(f=fname, e=excp)
            parsed_args.logger.error(msg)
            failed = True
    sys.exit(0 if not failed else 1)


def split_files(parsed_args):
    """
    Split log or SQL files
    """
    items = []
    for item in parsed_args.downloaded_files:
        if '*' in item:
            items.extend(glob.glob(item))
        else:
            items.append(item)
    parsed_args.downloaded_files = items
    if parsed_args.file_type == 'log':
        split_log_files(parsed_args)
    elif parsed_args.file_type == 'sql':
        split_sql_files(parsed_args)
    else:
        parsed_args.logger.error(
            'The split command does not support file type {ft}'.format(
                ft=parsed_args.file_type
            )
        )
        sys.exit(1)


def download_files(parsed_args):
    """
    Using the Namespace object generated by argparse, download the files
    that match the given criteria
    """
    start_year = int(parsed_args.begin_date[:4])
    end_year = int(parsed_args.end_date[:4])
    info = aws.BUCKETS.get(parsed_args.file_type)
    bucket = aws.make_s3_bucket(info['Bucket'])
    blobs = []
    for year in range(start_year, end_year + 1):
        prefix = info['Prefix'].format(
            site=parsed_args.site, year=year,
            date=parsed_args.begin_date, org=parsed_args.org,
            request=parsed_args.request_id or '',
        )
        blobs += aws.S3Blob.from_prefix(
            bucket=bucket, prefix=prefix
        )
    downloads = dict()
    for blob in blobs:
        fdate = aws.get_file_date(blob.name)
        if parsed_args.begin_date <= fdate <= parsed_args.end_date:
            fullname = os.path.join(
                parsed_args.destination,
                os.path.basename(os.path.join(*blob.name.split('/')))
            )
            downloads.setdefault(fullname, 0)
            parsed_args.logger.info(
                'Downloading {n} into {f}'.format(n=blob.name, f=fullname)
            )
            blob.download_file(fullname)
            downloads[fullname] += 1
            parsed_args.logger.info(
                'Done downloading {n}'.format(n=blob.name)
            )
            try:
                if parsed_args.file_type != 'sql':
                    parsed_args.logger.info(
                        'Decrypting {f}'.format(f=fullname)
                    )
                if parsed_args.file_type == 'email':
                    emails.process_email_file(
                        fname=fullname, verbose=parsed_args.verbose,
                        logger=parsed_args.logger,
                        timeout=parsed_args.decryption_timeout,
                    )
                    if parsed_args.verbose:
                        parsed_args.logger.info(
                            'Downloaded and decrypted {f}'.format(f=fullname)
                        )
                elif parsed_args.file_type == 'log':
                    downutils.decrypt_files(
                        fnames=fullname, verbose=parsed_args.verbose,
                        logger=parsed_args.logger,
                        timeout=parsed_args.decryption_timeout,
                    )
                    if parsed_args.verbose:
                        parsed_args.logger.info(
                            'Downloaded and decrypted {f}'.format(f=fullname)
                        )
                downloads[fullname] += 1
            except Exception as excp:
                parsed_args.logger.error(excp)
            cond = all((
                not parsed_args.keep_encrypted,
                parsed_args.file_type != 'sql'
            ))
            if cond:
                if downloads[fullname] == 2:
                    try:
                        os.remove(fullname)
                    except:
                        pass
    if not downloads:
        parsed_args.logger.warning(
            'No files found matching the given criteria'
        )
    if parsed_args.file_type == 'log' and parsed_args.split:
        parsed_args.downloaded_files = []
        for k, v in downloads.items():
            if v == 2:
                k, _ = os.path.splitext(k)
                parsed_args.downloaded_files.append(k)
        if not parsed_args.split_destination:
            parsed_args.destination = os.path.join(
                parsed_args.destination, 'TRACKING_LOGS'
            )
        else:
            parsed_args.destination = parsed_args.split_destination
        split_log_files(parsed_args)
    elif parsed_args.file_type == 'sql' and parsed_args.split:
        parsed_args.downloaded_files = list(downloads)
        if not parsed_args.split_destination:
            parsed_args.destination = os.path.join(
                parsed_args.destination, 'SQL'
            )
        else:
            parsed_args.destination = parsed_args.split_destination
        split_sql_files(parsed_args)
    rc = 0 if all(v == 2 for v in downloads.values()) else 1
    sys.exit(rc)


def push_to_bq(parsed_args):
    """
    Push to BigQuery
    """
    if not parsed_args.project:
        parsed_args.logger.error(
            'No GCP project given in the command line. '
            'None was found in config file(s) either. '
            'Aborting...'
        )
        sys.exit(1)
    if not parsed_args.items:
        parsed_args.logger.info('No items to process')
        sys.exit(0)
    parsed_args.logger.info('Connecting to BigQuery')
    try:
        if parsed_args.service_account_file is not None:
            client = gcp.BigqueryClient.from_service_account_json(
                parsed_args.service_account_file,
                project=parsed_args.project
            )
        else:
            client = gcp.BigqueryClient(
                project=parsed_args.project
            )
    except Exception as excp:
        errmsg = 'Failed to connect to BigQuery: {e}'
        parsed_args.logger.error(errmsg.format(e=excp))
        parsed_args.logger.error(
            'The error may be from an invalid service account file'
        )
        sys.exit(1)
    all_jobs = []
    for item in parsed_args.items:
        if not item.startswith('gs://') and not os.path.exists(item):
            errmsg = 'Skipping {f!r}. It does not exist.'
            parsed_args.logger.error(errmsg.format(f=item))
            if parsed_args.fail_fast:
                parsed_args.logger.error('Exiting...')
                sys.exit(1)
            continue
        if os.path.isdir(item):
            loader = client.load_tables_from_dir
            appender = all_jobs.extend
        else:
            loader = client.load_one_file_to_table
            appender = all_jobs.append
        parsed_args.logger.info(
            'Loading item(s) in {f!r} to BigQuery'.format(f=item)
        )
        appender(
            loader(
                item, parsed_args.file_type, parsed_args.project,
                parsed_args.create, parsed_args.append,
                parsed_args.use_storage, parsed_args.bucket
            )
        )
        parsed_args.logger.info(
            'Created BigQuery load job(s) for item(s) in {f!r}'.format(f=item)
        )
    if not all_jobs:
        errmsg = (
            'No items processed. Perhaps, the given directory is empty?'
        )
        parsed_args.logger.error(errmsg)
        sys.exit(1)
    if parsed_args.wait_for_loads:
        wait_for_bq_jobs(all_jobs)
    errors = 0
    for job in all_jobs:
        if job.errors:
            for err in client.extract_error_messages(job.errors):
                parsed_args.logger.error(err)
            errors += 1
    if errors:
        msg = (
            'Out of {j} load job(s) submitted, {f} failed to '
            'complete successfully'
        )
        parsed_args.logger.error(msg.format(
            j=len(all_jobs), f=errors
        ))
        sys.exit(1)
    if parsed_args.wait_for_loads:
        parsed_args.logger.info(
            '{c} item(s) loaded to BigQuery'.format(c=len(all_jobs))
        )
        sys.exit(0)
    msg = (
        '{c} BigQuery data load jobs started. Please consult your '
        'BigQuery console for more details about the status of said jobs.'
    )
    parsed_args.logger.info(msg.format(c=len(all_jobs)))


def push_to_gcs(parsed_args):
    """
    Push to Storage
    """
    if not parsed_args.bucket:
        parsed_args.logger.error(
            'No valid GCP bucket given in the command line. '
            'None was found in config file(s) either. '
            'Aborting...'
        )
        sys.exit(1)
    if not parsed_args.items:
        parsed_args.logger.info('No items to process')
        sys.exit(0)
    parsed_args.logger.info(
        'Connecting to Google Cloud Storage'
    )
    try:
        if parsed_args.service_account_file is not None:
            client = gcp.GCSClient.from_service_account_json(
                parsed_args.service_account_file,
                project=parsed_args.project
            )
        else:
            client = gcp.GCSClient(
                project=parsed_args.project
            )
    except Exception as excp:
        errmsg = 'Failed to connect to Google Cloud Storage: {e}'
        parsed_args.logger.error(errmsg.format(e=excp))
        parsed_args.logger.error(
            'The error may be from an invalid service account file'
        )
        sys.exit(1)
    failed = False
    for item in parsed_args.items:
        if not os.path.exists(item):
            errmsg = 'Skipping {f!r}. It does not exist.'
            parsed_args.logger.error(errmsg.format(f=item))
            if parsed_args.fail_fast:
                parsed_args.logger.error('Exiting...')
                sys.exit(1)
            continue
        if os.path.isdir(item):
            loader = client.load_dir
        else:
            loader = client.load_on_file_to_gcs
        try:
            parsed_args.logger.info(
                'Loading {f} to GCS'.format(f=item)
            )
            loader(
                item, parsed_args.file_type,
                parsed_args.bucket,
            )
            parsed_args.logger.info(
                'Done loading {f} to GCS'.format(f=item)
            )
        except Exception as excp:
            errmsg = 'Failed to load {f} to GCS: {e}'
            parsed_args.logger.error(errmsg.format(f=item, e=excp))
            parsed_args.logger.error(
                'The error may be from an invalid service account file'
            )
            if parsed_args.fail_fast:
                parsed_args.logger.error('Exiting...')
                sys.exit(1)
            failed = True
    sys.exit(1 if failed else 0)


def push_generated_files(parsed_args):
    """
    Using the Namespace object generated by argparse, push data files
    to a target destination
    """
    parsed_args.items = cli_utils.filter_generated_items(
        parsed_args.items, parsed_args.courses
    )
    if parsed_args.destination == 'bq':
        push_to_bq(parsed_args)
    else:
        push_to_gcs(parsed_args)


def make_secondary_tables(parsed_args):
    """
    Generate secondary datasets that rely on existing datasets
    and tables.
    """
    if not parsed_args.project:
        parsed_args.logger.error(
            'No GCP project given in the command line. '
            'None was found in config file(s) either. '
            'Aborting...'
        )
        sys.exit(1)
    if not parsed_args.course_ids:
        parsed_args.logger.info('No items to process')
        sys.exit(0)
    cond = all((
        'person_course' in parsed_args.tables,
        parsed_args.geo_table is None,
    ))
    if cond:
        parsed_args.logger.error(
            'person_course cannot be generated without a valid --geo-table'
            ' value provided.'
        )
        sys.exit(1)
    parsed_args.logger.info('Connecting to BigQuery')
    try:
        if parsed_args.service_account_file is not None:
            client = gcp.BigqueryClient.from_service_account_json(
                parsed_args.service_account_file,
                project=parsed_args.project
            )
        else:
            client = gcp.BigqueryClient(
                project=parsed_args.project
            )
    except Exception as excp:
        errmsg = 'Failed to connect to BigQuery: {e}'
        parsed_args.logger.error(errmsg.format(e=excp))
        parsed_args.logger.error(
            'The error may be from an invalid service account file'
        )
        sys.exit(1)
    parsed_args.logger.info('Connection established')
    all_jobs = dict()
    for course_id in parsed_args.course_ids:
        parsed_args.logger.info(
            'Making secondary tables for course ID {c}'.format(c=course_id)
        )
        for table_name in parsed_args.tables:
            job = make_table_from_sql(
                table=table_name, course_id=course_id, client=client,
                project=parsed_args.project, append=parsed_args.append,
                geo_table=parsed_args.geo_table,
                wait=parsed_args.wait_for_loads,
            )
            all_jobs[(course_id, table_name)] = job
        parsed_args.logger.info(
            'Submitted query jobs for course ID {c}'.format(c=course_id)
        )
    errors = 0
    parsed_args.logger.info('Checking for errors...')
    for (course_id, table), job in all_jobs.items():
        if job.errors:
            msg = 'Making {t} for {c} failed with the following: {e}'
            parsed_args.logger.error(msg.format(
                t=table, c=course_id,
                e='\n'.join(client.extract_error_messages(job.errors))
            ))
            errors += 1
    if errors:
        sys.exit(1)
    if parsed_args.wait_for_loads:
        msg = '{c} queries run and destination tables have been refreshed'
        parsed_args.logger.info(msg.format(c=len(all_jobs)))
        sys.exit(0)
    msg = (
        '{c} BigQuery query jobs started. Please consult your '
        'BigQuery console for more details about the status of said jobs.'
    )
    parsed_args.logger.info(msg.format(c=len(all_jobs)))


def main():
    """
    Entry point
    """
    global logger
    COMMANDS = {
        'list': list_files,
        'download': download_files,
        'split': split_files,
        'push': push_generated_files,
        'report': make_secondary_tables,
    }
    parser = ArgumentParser(
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--quiet', '-Q',
        help='Only print error messages to standard streams.',
        action='store_false',
        dest='verbose',
    )
    parser.add_argument(
        '--debug', '-B',
        help='Show some stacktrace if simeon stops because of a fatal error',
        action='store_true',
    )
    parser.add_argument(
        '--config-file', '-C',
        help=(
            'The INI configuration file to use for default arguments.'
        ),
    )
    parser.add_argument(
        '--log-file', '-L',
        help='Log file to use when simeon prints messages. Default: stdout',
        type=FileType('a'),
        default=sys.stdout,
    )
    subparsers = parser.add_subparsers(
        description='Choose a subcommand to carry out a task with simeon',
        dest='command'
    )
    subparsers.required = True
    downloader = subparsers.add_parser(
        'download',
        help='Download edX research data with the given criteria',
        description=(
            'Download edX research data with the given criteria below'
        )
    )
    downloader.set_defaults(command='download')
    downloader.add_argument(
        '--file-type', '-f',
        help='The type of files to get. Default: %(default)s',
        choices=['email', 'sql', 'log', 'rdx'],
        default='sql',
    )
    downloader.add_argument(
        '--destination', '-d',
        help='Directory where to download the file(s). Default: %(default)s',
        default=os.getcwd(),
    )
    downloader.add_argument(
        '--begin-date', '-b',
        help=(
            'Start date of the download timeframe. '
            'Default: %(default)s'
        ),
        default=aws.BEGIN_DATE,
        type=cli_utils.parsed_date
    )
    downloader.add_argument(
        '--end-date', '-e',
        help=(
            'End date of the download timeframe. '
            'Default: %(default)s'
        ),
        default=aws.END_DATE,
        type=cli_utils.parsed_date
    )
    downloader.add_argument(
        '--org', '-o',
        help='The organization whose data is fetched. Default: %(default)s',
        default='mitx',
    )
    downloader.add_argument(
        '--site', '-s',
        help='The edX site from which to pull data. Default: %(default)s',
        choices=['edge', 'edx', 'patches'],
        default='edx',
    )
    downloader.add_argument(
        '--split', '-S',
        help='Split downloaded SQL bundles or tracking logs',
        action='store_true',
    )
    downloader.add_argument(
        '--split-destination', '-D',
        help=(
            'The directory in which to put the split files if --split is '
            'given with this subcommand'
        ),
    )
    downloader.add_argument(
        '--dynamic-date', '-m',
        help=(
            'If splitting the downloaded files, use the '
            'dates from the records to make tracking log file names. '
            'Otherwise, the dates in the GZIP file names are used.'
        ),
        action='store_true',
    )
    downloader.add_argument(
        '--request-id', '-r',
        help='Request ID when listing RDX files',
    )
    downloader.add_argument(
        '--decryption-timeout', '-t',
        help='Number of seconds to wait for the decryption of files.',
        type=int,
    )
    cdgroup = downloader.add_mutually_exclusive_group(required=False)
    cdgroup.add_argument(
        '--courses', '-c',
        help=(
            'A list of white space separated course IDs whose data files '
            'are unpacked and decrypted.'
        ),
        nargs='*',
    )
    cdgroup.add_argument(
        '--clistings-file', '-l',
        help=(
            'Path to a file with one course ID per line. The file is expected'
            ' to have no header row.'
        ),
        type=cli_utils.courses_from_file,
    )
    downloader.add_argument(
        '--no-decryption', '-N',
        help='Don\'t decrypt the unpacked SQL files.',
        action='store_true',
    )
    downloader.add_argument(
        '--include-edge', '-E',
        help='Include the edge site files when splitting SQL data packages.',
        action='store_true',
    )
    downloader.add_argument(
        '--keep-encrypted', '-k',
        help='Keep the encrypted files after decrypting them',
        action='store_true',
    )
    lister = subparsers.add_parser(
        'list',
        help='List edX research data with the given criteria',
        description=(
            'List edX research data with the given criteria below'
        )
    )
    lister.set_defaults(command='list')
    lister.add_argument(
        '--file-type', '-f',
        help='The type of files to list. Default: %(default)s',
        choices=['email', 'sql', 'log', 'rdx'],
        default='sql',
    )
    lister.add_argument(
        '--begin-date', '-b',
        help=(
            'Start date of the listing timeframe. '
            'Default: %(default)s'
        ),
        default=aws.BEGIN_DATE,
        type=cli_utils.parsed_date
    )
    lister.add_argument(
        '--end-date', '-e',
        help=(
            'End date of the listing timeframe. '
            'Default: %(default)s'
        ),
        default=aws.END_DATE,
        type=cli_utils.parsed_date
    )
    lister.add_argument(
        '--org', '-o',
        help='The organization whose data is listed. Default: %(default)s',
        default='mitx',
    )
    lister.add_argument(
        '--site', '-s',
        help='The edX site from which to list data. Default: %(default)s',
        choices=['edge', 'edx', 'patches'],
        default='edx',
    )
    lister.add_argument(
        '--request-id', '-r',
        help='Request ID when listing RDX files',
    )
    lister.add_argument(
        '--json', '-j',
        help='Format the file listing in JSON',
        action='store_true',
    )
    splitter = subparsers.add_parser(
        'split',
        help='Split downloaded tracking log or SQL files',
        description='Split downloaded tracking log or SQL files'
    )
    splitter.set_defaults(command='split')
    splitter.add_argument(
        'downloaded_files',
        help='List of tracking log or SQL archives to split',
        nargs='+'
    )
    splitter.add_argument(
        '--file-type', '-f',
        help='The file type of the items provided. Default: %(default)s',
        default='log',
        choices=['log', 'sql'],
    )
    splitter.add_argument(
        '--no-decryption', '-N',
        help='Don\'t decrypt the unpacked SQL files.',
        action='store_true',
    )
    splitter.add_argument(
        '--jobs', '-j',
        help=(
            'Number of processes/threads to use when processing multiple '
            'files using multi threading or processing. Default: %(default)s'
        ),
        default=mp.cpu_count(),
        type=int,
    )
    splitter.add_argument(
        '--include-edge', '-E',
        help='Include the edge site files when splitting SQL data packages.',
        action='store_true',
    )
    splitter.add_argument(
        '--keep-encrypted', '-k',
        help='Keep the encrypted files after decrypting them',
        action='store_true',
    )
    splitter.add_argument(
        '--decryption-timeout', '-t',
        help='Number of seconds to wait for the decryption of files.',
        type=int,
    )
    splitter.add_argument(
        '--destination', '-d',
        help=(
            'Directory where to place the files from splitting the item(s).'
            ' Default: %(default)s'
        ),
        default=os.getcwd(),
    )
    csgroup = splitter.add_mutually_exclusive_group(required=False)
    csgroup.add_argument(
        '--courses', '-c',
        help=(
            'A list of white space separated course IDs whose data files '
            'are unpacked and decrypted.'
        ),
        nargs='*',
    )
    csgroup.add_argument(
        '--clistings-file', '-l',
        help=(
            'Path to a file with one course ID per line. The file is expected'
            ' to have no header row.'
        ),
        type=cli_utils.courses_from_file,
    )
    splitter.add_argument(
        '--dynamic-date', '-m',
        help=(
            'Use the dates from the records to make tracking log file names. '
            'Otherwise, the dates in the GZIP file names are used.'
        ),
        action='store_true',
    )
    pusher = subparsers.add_parser(
        'push',
        help='Push the generated data files to some target destination',
        description=(
            'Push to the given items to Google Cloud Storage or BigQuery'
        ),
    )
    pusher.set_defaults(command='push')
    pusher.add_argument(
        'destination',
        help='Sink for the generated data files',
        choices=['gcs', 'bq']
    )
    pusher.add_argument(
        'items',
        help='The items (file or folder) to push to GCS or BigQuery',
        nargs='+',
    )
    pusher.add_argument(
        '--project', '-p',
        help='GCP project associated with the target sink',
    )
    pusher.add_argument(
        '--bucket', '-b',
        help='GCS bucket name associated with the target sink',
        type=cli_utils.gcs_bucket,
    )
    pusher.add_argument(
        '--service-account-file', '-S',
        help='The service account to carry out the data load',
        type=cli_utils.optional_file
    )
    pusher.add_argument(
        '--max-bad-rows', '-m',
        help=(
            'Max number of bad rows to allow when loading data to BigQuery. '
            'Default: %(default)s'
        ),
        type=int,
        default=0,
    )
    pusher.add_argument(
        '--file-type', '-f',
        help='The type of files to push. Default: %(default)s',
        choices=['email', 'sql', 'log', 'rdx', 'cold'],
        default='sql',
    )
    pusher.add_argument(
        '--no-create', '-n',
        help=(
            'Don\'t create destination tables and datasets. '
            'They must already exist for the push operation '
            'to work.'
        ),
        action='store_false',
        dest='create',
    )
    pusher.add_argument(
        '--append', '-a',
        help=(
            'Whether to append to destination tables if they exist'
            ' when pushing data to BigQuery'
        ),
        action='store_true',
    )
    pusher.add_argument(
        '--use-storage', '-s',
        help='Whether to use GCS for actual files when loading to bq',
        action='store_true',
    )
    pusher.add_argument(
        '--fail-fast', '-F',
        help=(
            'Force simeon to quit as soon as an error is encountered'
            ' with any of the given items.'
        ),
        action='store_true',
    )
    pusher.add_argument(
        '--wait-for-loads', '-w',
        help=(
            'Wait for asynchronous BigQuery load jobs to finish. '
            'Otherwise, simeon creates load jobs and exits.'
        ),
        action='store_true',
    )
    cpgroup = pusher.add_mutually_exclusive_group(required=False)
    cpgroup.add_argument(
        '--courses', '-c',
        help=(
            'A list of white space separated course IDs whose data files '
            'are pushed to the target destination.'
        ),
        nargs='*',
    )
    cpgroup.add_argument(
        '--clistings-file', '-l',
        help=(
            'Path to a file with one course ID per line. The file is expected'
            ' to have no header row. '
            'Only files whose names match the course IDs are pushed.'
        ),
        type=cli_utils.course_paths_from_file,
    )
    reporter = subparsers.add_parser(
        'report',
        help='Make course reports using the datasets and tables in BigQuery',
        description=(
            'Make course reports using the datasets and tables in BigQuery'
        ),
    )
    reporter.set_defaults(command='report')
    reporter.add_argument(
        'course_ids',
        help='Course IDs whose secondary datasets are generated',
        nargs='+',
    )
    reporter.add_argument(
        '--project', '-p',
        help='GCP project associated with the tables to query',
    )
    reporter.add_argument(
        '--service-account-file', '-S',
        help='The service account to carry out the data load',
        type=cli_utils.optional_file
    )
    reporter.add_argument(
        '--append', '-a',
        help=(
            'Whether to append to destination tables if they exist'
        ),
        action='store_true',
    )
    reporter.add_argument(
        '--tables', '-t',
        help='table or tables to be processed. Default: %(default)s',
        nargs='*',
        default=[
            'video_axis', 'forum_events', 
            'problem_grades', 'chapter_grades', 
            'show_answer', 'video_stats_day',
            'show_answer_stats_by_user', 'show_answer_stats_by_course',
            'course_item', 'person_item', 
            'person_problem', 'course_problem',
            'person_course_day', 'pc_video_watched',
            'pc_day_totals', 'pc_day_trlang',
            'pc_day_ip_counts', 'language_multi_transcripts',
            'pc_nchapters', 'pc_forum',
            'course_modal_language', 'course_modal_ip',
            'forum_posts', 'forum_person',
            'enrollment_events', 'enrollday_all',
            'person_enrollment_verified', ''
            'person_course'
        ]
    )
    reporter.add_argument(
        '--geo-table', '-g',
        help=(
            'The fully qualified name of the geolocation table '
            'to join to modal_ip to extract geolocation information '
            'for IP addresses.'
        ),
        default='geocode.geoip',
        type=cli_utils.bq_table,
    )
    reporter.add_argument(
        '--youtube-table', '-y',
        help=(
            'The fully qualified name of the geolocation table '
            'to join to modal_ip to extract geolocation information '
            'for IP addresses.'
        ),
        default='videos.youtube',
        type=cli_utils.bq_table,
    )
    reporter.add_argument(
        '--fail-fast', '-F',
        help=(
            'Force simeon to quit as soon as an error is encountered'
            ' with any of the given items.'
        ),
        action='store_true',
    )
    reporter.add_argument(
        '--wait-for-loads', '-w',
        help=(
            'Wait for asynchronous BigQuery query jobs to finish. '
            'Otherwise, simeon creates query jobs and exits.'
        ),
        action='store_true',
    )
    args = parser.parse_args()
    args.logger = cli_utils.make_logger(
        verbose=args.verbose,
        stream=args.log_file,
        user='SIMEON:{cmd}'.format(cmd=args.command.upper()),
    )
    # Get simeon configurations and plug them in wherever
    # a CLI option is not given
    configs = cli_utils.find_config(args.config_file)
    for k, v in cli_utils.CONFIGS.items():
        for (attr, cgetter) in v:
            cli_arg = getattr(args, attr, None)
            config_arg = cgetter(configs, k, attr, fallback=None)
            if attr.replace('-', '_') == 'clistings_file':
                if args.command == 'push':
                    config_arg = cli_utils.course_paths_from_file(config_arg)
                else:
                    config_arg = cli_utils.courses_from_file(config_arg)
            if not cli_arg and config_arg:
                setattr(args, attr, config_arg)
    # Combine --courses with --clistings-file
    if hasattr(args, 'courses') and hasattr(args, 'clistings_file'):
        if not getattr(args, 'courses', None):
            args.courses = args.clistings_file
    # Signal handling for the usual interrupters
    # List shortened because Windows is too annoying
    # to know of many of the Unix interrupting signals.
    # Also, set the global logger variable, so the signal handler can use it.
    sigs = [signal.SIGABRT, signal.SIGTERM, signal.SIGINT]
    logger = args.logger
    if args.command == 'split':
        if len(args.downloaded_files) > 1 and not args.dynamic_date:
            for sig in sigs:
                signal.signal(sig, bail_out)
    # Call the function matching the given command
    try:
        COMMANDS.get(args.command)(args)
    except:
        _, excp, tb = sys.exc_info()
        if isinstance(excp, (EarlyExitError, SystemExit)):
            raise excp
        msg = 'The command {c} failed: {e}'
        if args.debug:
            traces = ['{e}'.format(e=excp)]
            traces += map(str.strip, traceback.format_tb(tb))
            msg = msg.format(c=args.command, e='\n'.join(traces))
        else:
            msg = msg.format(c=args.command, e=excp)
        args.logger.error(msg)
        sys.exit(1)


if __name__ == '__main__':
    main()
