"""
Extract the geolocation information of the IP addresses in the provided file
using a MaxMind version 2 geolocation database.
The point of this subcommand/plugin is to make a geolocation table of IPs to
location mapping, so that location information fields in person_course can be
populated.
"""
import csv
import gzip
import json
import os
import sys
import urllib.request as requests
from argparse import (
    ArgumentParser, FileType, RawDescriptionHelpFormatter
)

import simeon.scripts.utilities as cli_utils

try:
    from geoip2.database import Reader as GeoReader
except ImportError:
    GeoReader = None


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
    db, ip_file, outfile='geoip.json.gz',
    un_data=None, tracking_logs=False, logger=None,
):
    """
    Given a MaxMind DB and a credentials file to PROPROD,
    extract distinct IPs from obscured and get their location data

    :type fname: geoip2.database.Reader
    :param fname: MaxMind geolocation database Reader object to extract info
    :type ip_file: str
    :param ip_file: A file (text or tracking log) containing IP addresses
    :type outfile: str
    :param outfile: File in which to dump geolocation data
    :type un_data: Union[Dict[str, str], None]
    :param un_data: Dictionary containing UN country denomination information
    :type tracking_logs: bool
    :param tracking_logs: Whether or not the given IP file is a tracking log
    :type logger: Union[logging.Logger, None]
    :param logger: A logger object to log messages to specific streams
    :rtype: None
    :return: Writes data to the given output file in append mode
    """
    if tracking_logs:
        fh = gzip.open(ip_file, 'rt')
        reader = map(json.loads, fh)
    else:
        fh = open(ip_file)
        reader = csv.DictReader(fh, fieldnames=['ip'])
    with gzip.open(outfile, 'at') as outh:
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
                    'Record {i} from {f} could not be parsed: {e}'
                    '\nSkipping it...'
                ).format(i=line, f=ip_file, e=excp)
                if logger:
                    logger.warn(msg)
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
        'db',
        help='MaxMind version 2 geolocation database file.'
    )
    parser.add_argument(
        'ip_files',
        help=(
            'File(s) containing IP addresses. If it\'s a text file, '
            'it should be one IP address per record.\nIf the file is '
            'a tracking log file, then each record is assumed to have '
            'an "ip" string value.'
        ),
        nargs='+',
    )
    parser.add_argument(
        '--log-file', '-L',
        help='Log file to use when simeon prints messages. Default: stdout',
        type=FileType('w'),
        default=sys.stdout,
    )
    parser.add_argument(
        '--quiet', '-Q',
        help='Only print error messages to standard streams.',
        action='store_false',
        dest='verbose',
    )
    parser.add_argument(
        '--un-data', '-u',
        help=(
            'Path to a file with UN denominations. If no file is provided, '
            'one is downloaded from Github at '
            'https://raw.githubusercontent.com/mitodl/'
            'world_geographic_regions/master/'
            'geographic_regions_by_country.csv'
        ),
    )
    parser.add_argument(
        '--output', '-o',
        help='Output file name for the generated data. Default: %(default)s',
        default=os.path.join(os.getcwd(), 'geoip.json.gz'),
    )
    parser.add_argument(
        '--tracking-logs', '-t',
        help='Whether or not the given ip files are tracking log files',
        action='store_true',
    )
    args = parser.parse_args()
    args.logger = cli_utils.make_logger(
        user='SIMEON:GEOIP',
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
    try:
        os.remove(args.output)
    except OSError:
        pass
    try:
        un_denoms = import_un_denominations(args.un_data)
    except Exception as e:
        msg = 'Failed to get UN denominations because: {e}'
        if args.logger:
            args.logger.warn(msg.format(e=e))
        else:
            print(msg.format(e=e), file=sys.stderr)
        un_denoms = {}
    locs = GeoReader(args.db)
    for ip_file in args.ip_files:
        args.logger.info(
            'Processing IP addresses in {f} with geolocation DB {db}'.format(
                f=ip_file, db=args.db
            )
        )
        make_geo_data(
            db=locs, ip_file=ip_file,
            outfile=args.output, un_data=un_denoms,
            tracking_logs=args.tracking_logs,
            logger=args.logger,
        )
        args.logger.info(
            'Done with IP addresses in {f}'.format(
                f=ip_file,
            )
        )

if __name__ == '__main__':
    main()
