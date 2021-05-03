"""
simeon-youtube is a companion script to the simeon tool that helps with
extracting YouTube video details like title and duration from the course_axis
files generated by ``simeon split`` with a sql file type.
"""
import gzip
import json
import os
import re
import sys
import traceback
import urllib.request as request
from argparse import (
    ArgumentParser, FileType, RawDescriptionHelpFormatter
)

import simeon.scripts.utilities as cli_utils
import simeon.upload.gcp as gcp


API_URL = (
    'https://youtube.googleapis.com/youtube/v3/videos?'
    'part=contentDetails,snippet&id={ids}&key={token}'
)
VIDEO_COLS = {
    'id': ('id',), 'duration': ('contentDetails', 'duration'),
    'title': ('snippet', 'title'), 'description': ('snippet', 'description'),
    'channel_id': ('snippet', 'channelId'),
    'channel_title': ('snippet', 'channelTitle'),
    'published_at': ('snippet', 'publishedAt'),
}
DURATION_PATT = re.compile(r'^P(?P<date>\w*)T(?P<time>\w*)')
TIME_SECS = {
    'S': 1,
    'M': 60,
    'H': 3600,
}
DATE_SECS = {
    'D': 86400,
    'M': 2629800,
    'Y': 31557600,
}


def _batch_ids(files, size=50):
    """
    Batch YouTube video IDs by the given size
    from the list of file names.
    """
    out = []
    for file_ in files:
        with gzip.open(file_, 'rt') as fh:
            for line in fh:
                try:
                    line = json.loads(line)
                except Exception:
                    continue
                id_ = (line.get('data') or {}).get('ytid', '')
                if not id_:
                    continue
                out.append(id_.split(':', 2)[-1].strip())
                if len(out) >= size:
                    yield out
                    out = []
    if out:
        yield out


def _generate_request(ids, token):
    """
    Create a urllib.request.Request object
    with the given ids and token values
    and send a GET request to the global API_URL.
    All the exceptions raised by urllib are propagated downstream
    """
    joined_ids = ','.join(id_ for id_ in ids)
    url = API_URL.format(ids=joined_ids, token=token)
    headers = {
        # 'Authorization': 'Bearer {t}'.format(t=token),
        'Accept': 'application/json',
    }
    req = request.Request(url, headers=headers)
    return request.urlopen(req)


def _convert_and_time(val, times):
    """
    Convert the given value into an int and multiply by times
    """
    if not val:
        return 0
    try:
        return int(val) * times
    except Exception:
        return 0


def duration_to_seconds(duration):
    """
    Convert an ISO_8601 duration to seconds
    """
    durations = DURATION_PATT.search(duration)
    if not durations:
        return None
    out = 0
    names = {
        'time': TIME_SECS,
        'date': DATE_SECS,
    }
    for name, secs in names.items():
        chunk = durations.group(name)
        if not chunk:
            continue
        for char, times in secs.items():
            match = re.search(r'\d+(?={c})'.format(c=char), chunk)
            if not match:
                continue
            out += _convert_and_time(match.group(0), times)
    return out


def extract_video_info(parsed_args):
    """
    Extract YouTube video details from the course axis files
    whose paths are provided via the given Namespace object.
    """
    keys = ('youtube-token',)
    if not all(getattr(parsed_args, k.replace('-', '_'), None) for k in keys):
        msg = 'The following option(s) expected valid values: {o}'
        parsed_args.logger.error(msg.format(o=', '.join(keys)))
        sys.exit(1)
    outfile = gzip.open(parsed_args.output, 'wt')
    for chunk in _batch_ids(parsed_args.course_axes):
        try:
            resp = _generate_request(chunk, parsed_args.youtube_token)
        except Exception as excp:
            msg = 'Batch fetching YouTube video details failed with : {e}'
            parsed_args.logger.error(msg.format(e=json.load(excp.file)))
            continue
        data = json.load(resp)
        for item in data.get('items', []):
            record = {}
            for col, path in VIDEO_COLS.items():
                if len(path) == 1:
                    value = item.get(path[0])
                else:
                    subrec = (item.get(path[0]) or {})
                    for elm in path[1:-1]:
                        subrec = (subrec.get(elm) or {})
                    value = subrec.get(path[-1])
                record[col] = value
            record['duration'] = duration_to_seconds(record['duration'])
            outfile.write(json.dumps(record) + '\n')
    outfile.close()


def merge_video_data(parsed_args):
    """
    Merge a data file to its target BigQuery table.
    This is a two-step process:
    1. Create a temporary table in BigQuery and load the data file
    2. Merge the temporary to the target table on a given column
    """
    keys = ('youtube-table', 'column', 'project')
    if not all(getattr(parsed_args, k.replace('-', '_'), None) for k in keys):
        msg = 'The following options expected valid values: {o}'
        parsed_args.logger.error(msg.format(o=', '.join(keys)))
        sys.exit(1)
    parsed_args.logger.info(
        'Merging {f} to {t}'.format(
            f=parsed_args.youtube_file, t=parsed_args.youtube_table
        )
    )
    parsed_args.logger.info('Connecting to BigQuery')
    try:
        if parsed_args.service_account_file is not None:
            client = gcp.BigqueryClient.from_service_account_json(
                parsed_args.service_account_file,
                project=parsed_args.project
            )
        else:
            client = client = gcp.BigqueryClient(
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
    try:
        client.merge_to_table(
            fname=parsed_args.youtube_file, table=parsed_args.youtube_table,
            col=parsed_args.column,
            use_storage=parsed_args.youtube_file.startswith('gs://'),
        )
    except Exception as excp:
        msg = 'Merging {f} to {t} failed with the following: {e}'
        parsed_args.logger.error(
            msg.format(
                f=parsed_args.youtube_file,
                t=parsed_args.youtube_table,
                e=excp
            )
        )
        sys.exit(1)
    msg = 'Successfully merged the records in {f} to the table {t}'
    parsed_args.logger.info(
        msg.format(
            f=parsed_args.youtube_file, t=parsed_args.youtube_table
        )
    )


def unknown_command(parsed_args):
    """
    Exit the program if an unknown command is passed (somehow)
    """
    parsed_args.logger.error(
        'Unknow command {c}'.format(c=parsed_args.command)
    )
    parsed_args.logger.error('Exiting...')
    sys.exit(1)


def main():
    """
    simeon-youtube entry point
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
        '--debug', '-B',
        help='Show some stacktrace if simeon stops because of a fatal error',
        action='store_true',
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
    subparsers = parser.add_subparsers(
        description='A subcommand to carry out a task with simeon-youtube',
        dest='command'
    )
    subparsers.required = True
    extracter = subparsers.add_parser(
        'extract',
        help=(
            'Extract YouTube video details using the given API token and '
            'course axis json.gz files'
        ),
        description=(
            'Extract YouTube video details using the given API token and '
            'course axis json.gz files'
        )
    )
    extracter.add_argument(
        '--output', '-o',
        help=(
            'The .json.gz output file name for the YouTube video details. '
            'Default: %(default)s'
        ),
        default='youtube.json.gz',
    )
    extracter.add_argument(
        '--youtube-token', '-t',
        help=(
            'YouTube data V3 API token generated from the account hosting '
            'the lecture videos.'
        )
    )
    extracter.add_argument(
        'course_axes',
        help=(
            'course_axis.json.gz files from the simeon split process '
            'of SQL files'
        ),
        nargs='+',
    )
    merger = subparsers.add_parser(
        'merge',
        help=(
            'Merge the data file generated from simeon-youtube extract '
            'to the given target BigQuery table.'
        ),
        description=(
            'Merge the data file generated from simeon-youtube extract '
            'to the given target BigQuery table.'
        )
    )
    merger.add_argument(
        'youtube_file',
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
        '--youtube-table', '-y',
        help='The target table where the YouTube video details are stored.',
        default='videos.youtube',
        type=cli_utils.bq_table,
    )
    merger.add_argument(
        '--column', '-c',
        help=(
            'The column on which to to merge the file and table. '
            'Default: %(default)s'
        ),
        default='id',
    )
    args = parser.parse_args()
    args.logger = cli_utils.make_logger(
        user='SIMEON-YOUTUBE:{c}'.format(c=args.command.upper()),
        verbose=args.verbose,
        stream=args.log_file,
    )
    configs = cli_utils.find_config(args.config_file)
    for k, v in cli_utils.CONFIGS.items():
        for (attr, cgetter) in v:
            cli_arg = getattr(args, attr, None)
            config_arg = cgetter(configs, k, attr, fallback=None)
            if not cli_arg and config_arg:
                setattr(args, attr, config_arg)
    COMMANDS = {
        'extract': extract_video_info,
        'merge': merge_video_data,
    }
    try:
        COMMANDS.get(args.command, unknown_command)(args)
    except Exception as excp:
        _, excp, tb = sys.exc_info()
        if isinstance(excp, SystemExit):
            raise excp
        msg = 'The command {c} failed: {e}'
        if args.debug:
            traces = ['{e}'.format(e=excp)]
            traces += map(str.strip, traceback.format_tb(tb))
            msg = msg.format(c=args.command, e='\n'.join(traces))
        else:
            msg = msg.format(c=args.command, e=excp)
        # msg = 'The command {c} failed with: {e}'
        args.logger.error(msg.format(c=args.command, e=excp))
        sys.exit(1)


if __name__ == '__main__':
    main()