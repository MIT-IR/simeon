"""
simeon is a command line tool that helps with processing edx data
"""
import os
import sys
from argparse import ArgumentParser

import simeon.download.aws as aws


def main():
    """
    Entry point
    """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        '--file-type', '-f',
        help='The type of files to get. Default: %(default)s',
        choices=['email', 'sql', 'log'],
        default='sql',
    )
    parser.add_argument(
        '--destination', '-d',
        help='Directory where to download the file(s). Default: %(default)s',
        default=os.getcwd(),
    )
    parser.add_argument(
        '--threshold-date', '-t',
        help=(
            'Download files as or more recent than this date. '
            'Default: %(default)s'
        ),
        default=aws.DATE,
    )
    parser.add_argument(
        '--org', '-o',
        help='The organization whose data is fetched. Default: %(default)s',
        default='mitx',
    )
    parser.add_argument(
        '--site', '-s',
        help='The edX site from which to pull data. Default: %(default)s',
        choices=['edge', 'edx', 'patches'],
        default='edx',
    )
    parser.add_argument(
        '--quiet', '-q',
        help='Only print error messages to standard streams.',
        action='store_true',
    )
    args = parser.parse_args()
    args.year = args.threshold_date[:4]
    args.verbose = not args.quiet
    info = aws.BUCKETS.get(args.file_type)
    info['Prefix'] = info['Prefix'].format(
        site=args.site, year=args.year,
        date=args.threshold_date, org=args.org
    )
    bucket = aws.make_s3_bucket(info['Bucket'])
    blobs = aws.S3Blob.from_prefix(bucket=bucket, prefix=info['Prefix'])
    fcount = 0
    decrypt_count = 0
    for blob in blobs:
        fdate = aws.get_file_date(blob.name)
        if args.threshold_date > fdate:
            continue
        fullname = blob.download_file()
        fcount += 1
        try:
            if args.file_type == 'email':
                aws.process_email_file(fullname, args.verbose)
            else:
                aws.decrypt_file(fullname, args.verbose)
            decrypt_count += 1
            if args.verbose:
                print('Downloaded and decrypted {f}'.format(f=fullname))
        except Exception as excp:
            print(excp, file=sys.stderr)
    if fcount == 0:
        print('No files found matching the given criteria')
    rc = 0 if fcount == decrypt_count else 1
    sys.exit(rc)


if __name__ == '__main__':
    main()
