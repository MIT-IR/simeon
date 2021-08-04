"""
simeon-geoip is a companion script to the simeon tool that helps with
extracting geolocation information from a MaxMind database, and merging
the generated data file to a target table in BigQuery.

To extract geolocation information for IP addresses in a given file, you
can invoke it as follows:

simeon-geoip extract -t -o geo.json.gz ${maxmind_db} ${tracking_log_files}

The above line should generate geolocation data and put it in geo.json.gz
using the given tracking log files and the version 2 MaxMind DB.

To merge the generated file to a target BigQuery table, invoke it as follows:

simeon-geoip merge -t ${project}.${dataset}.${geotable} -p ${project} \\
    -S ${service_account_file} geo.json.gz

The above command will merge the given file into the target table given by the
-t option. If the target table does not exist, then the given is loaded directly
into it.
"""
import csv
import glob
import gzip
import json
import os
import sys
import urllib.request as requests
from argparse import (
    ArgumentParser, FileType, RawDescriptionHelpFormatter
)
from datetime import datetime

import simeon
import simeon.scripts.utilities as cli_utils
import simeon.upload.gcp as gcp

try:
    from geoip2.database import Reader as GeoReader
except ImportError:
    GeoReader = None


SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'upload', 'schemas'
)


def import_un_denominations(fname=None):
    """
    Import UN denominations data from GitHub if fname is
    not provided.
    Generate records with said data by mapping country 2-letter ISO codes
    to their UN denominations.
    """
    header = [
        'iso_code', 'code', 'name', 'un_major_region',
        'continent', 'un_economic_group', 'un_developing_nation',
        'un_special_region'
    ]
    if fname:
        fp = open(fname)
        next(fp)
    else:
        url = (
            'https://raw.githubusercontent.com/mitodl/'
            'world_geographic_regions/master/'
            'geographic_regions_by_country.csv'
        )
        req = requests.Request(url, method='GET')
        resp = requests.urlopen(req)
        fp = resp.fp
        next(fp)
        fp = (l.decode('utf8', 'ignore') for l in fp)
    reader = csv.DictReader(fp, delimiter=',', fieldnames=header)
    out = dict()
    for row in reader:
        out[row.get('iso_code')] = dict((k, row[k]) for k in header[3:])
    return out


def make_geo_data(
    db, ip_files, outfile='geoip.json.gz',
    un_data=None, tracking_logs=False, logger=None,
):
    """
    Given a MaxMind DB and a credentials file to PROPROD,
    extract distinct IPs from obscured and get their location data

    :type fname: geoip2.database.Reader
    :param fname: MaxMind geolocation database Reader object to extract info
    :type ip_file: Iterable[str]
    :param ip_file: List of files (text or tracking log) containing IPs
    :type outfile: str
    :param outfile: File in which to dump geolocation data
    :type un_data: Union[Dict[str, str], None]
    :param un_data: Dictionary containing UN country denomination information
    :type tracking_logs: bool
    :param tracking_logs: Whether or not the given IP files are tracking logs
    :type logger: Union[logging.Logger, None]
    :param logger: A logger object to log messages to specific streams
    :rtype: None
    :returns: Writes data to the given output file in append mode
    """
    with gzip.open(outfile, 'at') as outh:
        for ip_file in ip_files:
            if tracking_logs:
                fh = gzip.open(ip_file, 'rt')
                reader = map(json.loads, fh)
            else:
                fh = open(ip_file)
                reader = csv.DictReader(fh, fieldnames=['ip'])
            seen = set()
            line = 0
            while True:
                line += 1
                try:
                    rec = next(reader)
                except StopIteration:
                    break
                except Exception as excp:
                    msg = (
                        'Record {i} from {f} could not be parsed: {e}. '
                        'Skipping it...'
                    ).format(i=line, f=ip_file, e=excp)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg, file=sys.stderr)
                    continue
                ip_address = rec.get('ip')
                if not ip_address or ip_address in seen:
                    continue
                seen.add(ip_address)
                try:
                    info = db.city(ip_address)
                    un_info = un_data.get(info.country.iso_code, {})
                    subdiv = info.subdivisions.most_specific
                    row = {
                        'ip': ip_address,
                        'city': info.city.names.get('en'),
                        'countryLabel': info.country.names.get('en'),
                        'country': info.country.iso_code,
                        'cc_by_ip': info.country.iso_code,
                        'postalCode': info.postal.code,
                        'continent': info.continent.names.get('en'),
                        'subdivision': subdiv.names.get('en'),
                        'region': subdiv.iso_code,
                        'latitude': info.location.latitude,
                        'longitude': info.location.longitude,
                    }
                    row.update(un_info)
                except Exception:
                    row = {
                        'ip': ip_address,
                    }
                outh.write(json.dumps(row) + '\n')
            fh.close()


def main():
    """
    geoip entry point
    """
    parser = ArgumentParser(
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--log-file', '-L',
        help='Log file to use when simeon prints messages. Default: stdout',
        type=FileType('a'),
        default=sys.stdout,
    )
    parser.add_argument(
        '--quiet', '-Q',
        help='Only print error messages to standard streams.',
        action='store_false',
        dest='verbose',
    )
    parser.add_argument(
        '--config-file', '-C',
        help=(
            'The INI configuration file to use for default arguments.'
        ),
    )
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='%(prog)s {v}'.format(v=simeon.__version__)
    )
    subparsers = parser.add_subparsers(
        description='Choose a subcommand to carry out a task with simeon-geoip',
        dest='command'
    )
    subparsers.required = True
    extracter = subparsers.add_parser(
        'extract',
        help='Extract geolocation information from the given MaxMind DB',
        description=(
            'Extract geolocation information for the IP addresses '
            'in the given IP files from the MaxMind version2 database'
        )
    )
    extracter.add_argument(
        'db',
        help='MaxMind version 2 geolocation database file.'
    )
    extracter.add_argument(
        'ip_files',
        help=(
            'File(s) containing IP addresses. If it\'s a text file, '
            'it should be one IP address per record.\n\nIf the file is '
            'a tracking log file, then each record is assumed to have '
            'an "ip" string value.'
        ),
        nargs='+',
    )
    extracter.add_argument(
        '--un-data', '-u',
        help=(
            'Path to a file with UN denominations. If no file is provided, '
            'one is downloaded from Github at '
            'https://raw.githubusercontent.com/mitodl/'
            'world_geographic_regions/master/'
            'geographic_regions_by_country.csv'
        ),
    )
    extracter.add_argument(
        '--output', '-o',
        help='Output file name for the generated data. Default: %(default)s',
        default=os.path.join(
            os.getcwd(),
            'geoip_{dt}.json.gz'.format(
                dt=datetime.now().strftime('%Y%m%d%H%M%S')
            )
        ),
    )
    extracter.add_argument(
        '--tracking-logs', '-t',
        help='Whether or not the given ip files are tracking log files',
        action='store_true',
    )
    merger = subparsers.add_parser(
        'merge',
        help='Merge the given file to a target BigQuery table name',
        description=(
            'Merge the given file to a target BigQuery table name'
        )
    )
    merger.add_argument(
        'geofile',
        help='A .json.gz file generated from the extract command'
    )
    merger.add_argument(
        '--project', '-p',
        help='The BigQuery project id where the target table resides.'
    )
    merger.add_argument(
        '--service-account-file', '-S',
        help='The service account file to use when connecting to BigQuery'
    )
    merger.add_argument(
        '--geo-table', '-g',
        help='The target table where the geolocation data are stored.',
        default='geocode.geoip',
        type=cli_utils.bq_table,
    )
    merger.add_argument(
        '--column', '-c',
        help=(
            'The column on which to to merge the file and table. '
            'Default: %(default)s'
        ),
        default='ip',
    )
    merger.add_argument(
        '--schema-dir', '-s',
        help=(
            'Directory where schema file are found. '
            'Default: {d}'.format(d=SCHEMA_DIR)
        ),
    )
    merger.add_argument(
        '--update-description', '-u',
        help=(
            'Update the description of the destination table with '
            'the "description" value from the corresponding schema file'
        ),
        action='store_true',
    )
    args = parser.parse_args()
    args.logger = cli_utils.make_logger(
        user='SIMEON-GEOIP:{c}'.format(c=args.command.upper()),
        verbose=args.verbose,
        stream=args.log_file,
    )
    if not GeoReader:
        args.logger.error(
            'simeon was installed without geoip2. '
            'please reinstall it with python -m pip install simeon[geoip]. '
            'Or, install geoip2 with python -m pip install geoip2.'
        )
        sys.exit(1)
    if args.command == 'extract':
        files = []
        for file_ in args.ip_files:
            if '*' in file_:
                files.extend(glob.iglob(file_))
            else:
                files.append(file_)
        args.ip_files = files
        if not args.ip_files:
            msg = 'No valid IP data provided. Exiting...'
            if args.logger:
                args.logger.error(msg)
            else:
                print(msg, file=sys.stderr)
        try:
            os.remove(args.output)
        except OSError:
            pass
        try:
            un_denoms = import_un_denominations(args.un_data)
        except Exception as e:
            msg = 'Failed to get UN denominations because: {e}'
            if args.logger:
                args.logger.warning(msg.format(e=e))
            else:
                print(msg.format(e=e), file=sys.stderr)
            un_denoms = {}
        locs = GeoReader(args.db)
        make_geo_data(
            db=locs, ip_files=args.ip_files,
            outfile=args.output, un_data=un_denoms,
            tracking_logs=args.tracking_logs,
            logger=args.logger,
        )
        args.logger.info('Done processing the given IP files')
    else:
        try:
            configs = cli_utils.find_config(args.config_file)
        except Exception as excp:
            args.logger.error(str(excp).replace('\n', ' '))
            sys.exit(1)
        for k, v in cli_utils.CONFIGS.items():
            for (attr, cgetter) in v:
                cli_arg = getattr(args, attr, None)
                config_arg = cgetter(configs, k, attr, fallback=None)
                if not cli_arg and config_arg:
                    setattr(args, attr, config_arg)
        keys = ('geo-table', 'column', 'project')
        if not all(getattr(args, k.replace('-', '_'), None) for k in keys):
            msg = 'The following options expected valid values: {o}'
            args.logger.error(msg.format(o=', '.join(keys)))
            sys.exit(1)
        args.logger.info(
            'Merging {f} to {t}'.format(f=args.geofile, t=args.geo_table)
        )
        args.logger.info('Connecting to BigQuery')
        try:
            if args.service_account_file is not None:
                client = gcp.BigqueryClient.from_service_account_json(
                    args.service_account_file,
                    project=args.project
                )
            else:
                client = client = gcp.BigqueryClient(
                    project=args.project
                )
        except Exception as excp:
            errmsg = 'Failed to connect to BigQuery: {e}'
            args.logger.error(errmsg.format(e=excp))
            args.logger.error(
                'The error may be from an invalid service account file'
            )
            sys.exit(1)
        args.logger.info('Connection established')
        try:
            client.merge_to_table(
                fname=args.geofile, table=args.geo_table, col=args.column,
                use_storage=args.geofile.startswith('gs://'),
                schema_dir=args.schema_dir, patch=args.update_description,
            )
        except Exception as excp:
            msg = 'Merging {f} to {t} failed with the following: {e}'
            args.logger.error(
                msg.format(f=args.geofile, t=args.geo_table, e=excp)
            )
            sys.exit(1)
        msg = 'Successfully merged the records in {f} to the table {t}'
        args.logger.info(msg.format(f=args.geofile, t=args.geo_table))


if __name__ == '__main__':
    main()
