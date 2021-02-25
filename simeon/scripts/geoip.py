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
from argparse import ArgumentParser, RawDescriptionHelpFormatter

try:
    from geoip2.database import Reader as GeoReader
except ImportError:
    sys.exit(
        'simeon was installed without geoip2. '
        'please reinstall it with python -m pip install simeon[geoip]. '
        'Or, install geoip2 with python -m pip install geoip2.'
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


def make_geo_data(fname, ip_file, outfile='geoip.json.gz', un_data=None):
    """
    Given a MaxMind DB and a credentials file to PROPROD,
    extract distinct IPs from obscured and get their location data
    """
    try:
        un_denoms = import_un_denominations(un_data)
    except Exception as e:
        msg = 'Failed to get UN denominations because: {e}'
        print(msg.format(e=e), file=sys.stderr)
        un_denoms = {}
    locs = GeoReader(fname)
    with open(ip_file) as fh, open(outfile, 'wt') as outh:
        reader = csv.reader(fh)
        for rec in reader:
            try:
                info = locs.city(rec[0])
                un_info = un_denoms.get(info.country.iso_code, {})
                subdiv = info.subdivisions.most_specific
                row = {
                    'ip': rec[0],
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
                    'ip': rec[0],
                }
            outh.write(json.dumps(row) + '\n')


def main():
    """
    geoip entry point
    """
    parser = ArgumentParser(
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument('db', help='MaxMind Geo DB')
    parser.add_argument(
        'ip_file',
        help='A file with IP addresses. One IP address per line',
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
        default=os.path.join(os.getcwd(), 'geoip.json.gz')
    )
    args = parser.parse_args()
    make_geo_data(args.db, args.ip_file, args.output, args.un_data)


if __name__ == '__main__':
    main()
