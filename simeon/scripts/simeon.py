"""
simeon is a command line tool that helps with processing edx data
"""
import os
import sys
import traceback
from argparse import ArgumentParser

import simeon.download.aws as aws
import simeon.download.logs as logs
import simeon.scripts.utilities as cli_utils


def list_files(parsed_args):
    """
    Using the Namespace object generated by argparse, list the files
    that match the given criteria
    """
    parsed_args.year = parsed_args.begin_date[:4]
    parsed_args.verbose = not parsed_args.quiet
    info = aws.BUCKETS.get(parsed_args.file_type)
    info['Prefix'] = info['Prefix'].format(
        site=parsed_args.site, year=parsed_args.year,
        date=parsed_args.begin_date, org=parsed_args.org
    )
    bucket = aws.make_s3_bucket(info['Bucket'])
    blobs = aws.S3Blob.from_prefix(bucket=bucket, prefix=info['Prefix'])
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
    failed = False
    for fname in parsed_args.tracking_logs:
        try:
            logs.split_tracking_log(fname, parsed_args.destination)
        except Exception as excp:
            _, _, tb = sys.exc_info()
            traces = '\n'.join(map(str.strip, traceback.format_tb(tb)))
            failed = True
            msg = 'Failed to split {f}: {e}'.format(f=fname, e=traces)
            print(msg, file=sys.stderr)
    sys.exit(0 if not failed else 1)


def download_files(parsed_args):
    """
    Using the Namespace object generated by argparse, download the files
    that match the given criteria
    """
    parsed_args.year = parsed_args.begin_date[:4]
    parsed_args.verbose = not parsed_args.quiet
    info = aws.BUCKETS.get(parsed_args.file_type)
    info['Prefix'] = info['Prefix'].format(
        site=parsed_args.site, year=parsed_args.year,
        date=parsed_args.begin_date, org=parsed_args.org
    )
    bucket = aws.make_s3_bucket(info['Bucket'])
    blobs = aws.S3Blob.from_prefix(bucket=bucket, prefix=info['Prefix'])
    downloads = dict()
    for blob in blobs:
        fdate = aws.get_file_date(blob.name)
        if parsed_args.begin_date <= fdate <= parsed_args.end_date:
            fullname = os.path.join(
                parsed_args.destination,
                os.path.basename(os.path.join(*blob.name.split('/')))
            )
            downloads.setdefault(fullname, 0)
            blob.download_file(fullname)
            downloads[fullname] += 1
            try:
                if parsed_args.file_type == 'email':
                    aws.process_email_file(fullname, parsed_args.verbose)
                else:
                    aws.decrypt_file(fullname, parsed_args.verbose)
                    downloads[fullname] += 1
                if parsed_args.verbose:
                    print('Downloaded and decrypted {f}'.format(f=fullname))
                if parsed_args.file_type == 'log' and parsed_args.split:
                    logs.split_tracking_log(fullname, parsed_args.destination)
            except Exception as excp:
                print(excp, file=sys.stderr)
    if not downloads:
        print('No files found matching the given criteria')
    if parsed_args.file_type == 'log' and parsed_args.split:
        parsed_args.tracking_logs = []
        for k, v in downloads.items():
            if v == 2:
                parsed_args.tracking_logs.append(k)
        split_log_files(parsed_args)
    rc = 0 if all(v == 2 for v in downloads.values()) else 1
    sys.exit(rc)



def push_generated_files(parsed_args):
    """
    Using the Namespace object generated by argparse, push data files
    to a target destination
    """
    msg = (
        'Push has not yet been implemented.\n'
        'When ready, your data will be pushed to {d} with info:\n'
        'Project: {p} - Bucket: {b}'
    )
    print(
        msg.format(
            p=parsed_args.project, b=parsed_args.bucket,
            d=parsed_args.destination.upper()
        )
    )
    sys.exit(0)


def main():
    """
    Entry point
    """
    COMMANDS = {
        'list': list_files,
        'download': download_files,
        'split': split_log_files,
        'push': push_generated_files,
    }
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        '--quiet', '-q',
        help='Only print error messages to standard streams.',
        action='store_true',
    )
    subparsers = parser.add_subparsers(
        description='Choose a subcommand to carry out a task with simeon',
        dest='command'
    )
    subparsers.required = True
    downloader = subparsers.add_parser(
        'download',
        help='Download edX research data with the given criteria'
    )
    downloader.set_defaults(command='download')
    downloader.add_argument(
        '--file-type', '-f',
        help='The type of files to get. Default: %(default)s',
        choices=['email', 'sql', 'log'],
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
        help='Split downloaded log files',
        action='store_true',
    )
    lister = subparsers.add_parser(
        'list',
        help='List edX research data with the given criteria'
    )
    lister.set_defaults(command='list')
    lister.add_argument(
        '--file-type', '-f',
        help='The type of files to list. Default: %(default)s',
        choices=['email', 'sql', 'log'],
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
        '--json', '-j',
        help='The edX site from which to list data. Default: %(default)s',
        action='store_true',
    )
    splitter = subparsers.add_parser(
        'split',
        help='Split downloaded tracking log files'
    )
    splitter.set_defaults(command='split')
    splitter.add_argument(
        'tracking_logs',
        help='List of tracking log files to split',
        nargs='+'
    )
    splitter.add_argument(
        '--destination', '-d',
        help='Directory where to download the file(s). Default: %(default)s',
        default=os.getcwd(),
    )
    pusher = subparsers.add_parser(
        'push',
        help='Push the generated data files to some target destination'
    )
    pusher.set_defaults(command='push')
    pusher.add_argument(
        'destination',
        help='Sink for the generated data files',
        choices=['gcs', 'bq']
    )
    pusher.add_argument(
        '--project', '-p',
        help='GCP project associated with the target sink',
        required=True
    )
    pusher.add_argument(
        '--bucket', '-b',
        help='GCS bucket name associated with the target sink',
        required=True,
        type=cli_utils.gcs_bucket,
    )
    args = parser.parse_args()
    COMMANDS.get(args.command)(args)


if __name__ == '__main__':
    main()
