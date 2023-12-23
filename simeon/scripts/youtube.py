"""
simeon-youtube is a companion script to the simeon tool that helps with
extracting YouTube video details like title and duration from the course_axis
files generated by ``simeon split`` with a sql file type.
"""
import glob
import gzip
import json
import os
import re
import sys
import traceback
import urllib.request as request
from argparse import ArgumentTypeError, FileType, RawDescriptionHelpFormatter
from datetime import datetime

import simeon
import simeon.scripts.utilities as cli_utils
import simeon.upload.gcp as gcp

SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "upload", "schemas")
API_URL = "https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails,snippet&id={ids}&key={token}"
VIDEO_COLS = {
    "id": ("id",),
    "duration": ("contentDetails", "duration"),
    "title": ("snippet", "title"),
    "description": ("snippet", "description"),
    "channel_id": ("snippet", "channelId"),
    "channel_title": ("snippet", "channelTitle"),
    "published_at": ("snippet", "publishedAt"),
}
DURATION_PATT = re.compile(r"^P(?P<date>\w*)T(?P<time>\w*)")
TIME_SECS = {
    "S": 1,
    "M": 60,
    "H": 3600,
}
DATE_SECS = {
    "D": 86400,
    "M": 2629800,
    "Y": 31557600,
}


def batch_size_type(val):
    """
    Make sure that val is an integer and is between 5 and 50.
    This is used for --batch-size to indicate the range of batch sizes
    allowed.
    """
    try:
        val = int(val)
    except Exception:
        raise ArgumentTypeError(f"{val} is not a valid integer value. Please provide a valid number.")
    if val < 5 or val > 50:
        raise ArgumentTypeError("Expected batch sizes range between 5 and 50")
    return val


def _batch_ids(files, size=10):
    """
    Batch YouTube video IDs by the given size
    from the list of file names.
    """
    out = []
    seen = set()
    for file_ in files:
        for path in glob.iglob(file_):
            with gzip.open(path, "rt") as fh:
                for line in fh:
                    try:
                        line = json.loads(line)
                    except Exception:
                        continue
                    id_ = (line.get("data") or {}).get("ytid", "")
                    if not id_:
                        continue
                    id_ = id_.split(":", 2)[-1].strip()
                    if id_ in seen:
                        continue
                    out.append(id_)
                    seen.add(id_)
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
    if isinstance(ids, (tuple, list, dict)):
        ids = ",".join(ids)
    url = API_URL.format(ids=ids, token=token)
    headers = {
        # 'Authorization': 'Bearer {t}'.format(t=token),
        "Accept": "application/json",
    }
    req = request.Request(url, headers=headers)
    return request.urlopen(req)


def _process_one_by_one(ids, token, logger):
    """
    This steps through the video IDs and processes them one at a time
    """
    data = dict(items=[])
    for id_ in ids:
        try:
            items = json.load(_generate_request(id_, token)).get("items", [])
            data["items"] += items
        except Exception as excp:
            msg = "Error processing video ID {i}: {e}"
            fh = getattr(excp, "file", None)
            if fh is None:
                logger.error(msg.format(i=id_, e=excp))
            else:
                logger.error(msg.format(i=id_, e=json.load(fh)))
    return data


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
        "time": TIME_SECS,
        "date": DATE_SECS,
    }
    for name, secs in names.items():
        chunk = durations.group(name)
        if not chunk:
            continue
        for char, times in secs.items():
            match = re.search(r"\d+(?={c})".format(c=char), chunk)
            if not match:
                continue
            out += _convert_and_time(match.group(0), times) or 0
    return out


def extract_video_info(parsed_args):
    """
    Extract YouTube video details from the course axis files
    whose paths are provided via the given Namespace object.
    """
    keys = ("youtube-token",)
    seen = set()
    if not all(getattr(parsed_args, k.replace("-", "_"), None) for k in keys):
        msg = "The following option(s) expected valid values: {o}"
        parsed_args.logger.error(msg.format(o=", ".join(keys)))
        sys.exit(1)
    os.makedirs(os.path.dirname(parsed_args.output), exist_ok=True)
    outfile = gzip.open(parsed_args.output, "wt")
    for chunk in _batch_ids(parsed_args.course_axes, parsed_args.batch_size):
        try:
            resp = _generate_request(chunk, parsed_args.youtube_token)
            data = json.load(resp)
        except Exception:
            data = _process_one_by_one(chunk, parsed_args.youtube_token, parsed_args.logger)
        if not data:
            continue
        for item in data.get("items", []):
            record = {}
            for col, path in VIDEO_COLS.items():
                if len(path) == 1:
                    value = item.get(path[0])
                else:
                    subrec = item.get(path[0]) or {}
                    for elm in path[1:-1]:
                        subrec = subrec.get(elm) or {}
                    value = subrec.get(path[-1])
                record[col] = value
            if record.get("id") in seen:
                continue
            record["duration"] = duration_to_seconds(record["duration"])
            record["timestamp"] = str(datetime.utcnow())
            outfile.write(json.dumps(record) + "\n")
            seen.add(record.get("id"))
    outfile.close()


def merge_video_data(parsed_args):
    """
    Merge a data file to its target BigQuery table.
    This is a two-step process:
    1. Create a temporary table in BigQuery and load the data file
    2. Merge the temporary to the target table on a given column
    """
    keys = ("youtube-table", "column", "project")
    if not all(getattr(parsed_args, k.replace("-", "_"), None) for k in keys):
        msg = "The following options expected valid values: {o}"
        parsed_args.logger.error(msg.format(o=", ".join(keys)))
        sys.exit(1)
    parsed_args.logger.info("Merging {f} to {t}".format(f=parsed_args.youtube_file, t=parsed_args.youtube_table))
    parsed_args.logger.info("Connecting to BigQuery")
    try:
        if parsed_args.service_account_file is not None:
            client = gcp.BigqueryClient.from_service_account_json(
                parsed_args.service_account_file, project=parsed_args.project
            )
        else:
            client = gcp.BigqueryClient(project=parsed_args.project)
    except Exception as excp:
        errmsg = "Failed to connect to BigQuery: {e}"
        parsed_args.logger.error(errmsg.format(e=excp))
        sys.exit(1)
    parsed_args.logger.info("Connection established")
    try:
        client.merge_to_table(
            fname=parsed_args.youtube_file,
            table=parsed_args.youtube_table,
            col=parsed_args.column,
            use_storage=parsed_args.youtube_file.startswith("gs://"),
            schema_dir=parsed_args.schema_dir,
            patch=parsed_args.update_description,
            match_equal_columns=parsed_args.match_equal_columns,
            match_unequal_columns=parsed_args.match_unequal_columns,
        )
    except Exception:
        _, excp, tb = sys.exc_info()
        context = getattr(excp, "context_dict", {})
        context["youtube_file"] = parsed_args.youtube_file
        context["youtube_table"] = parsed_args.youtube_table
        msg = "Merging {f} to {t} failed with the following: {e}"
        if parsed_args.debug:
            traces = ["{e}".format(e=excp)]
            traces += map(str.strip, traceback.format_tb(tb))
            msg = msg.format(
                e="\n".join(traces),
                f=parsed_args.youtube_file,
                t=parsed_args.youtube_table,
            )
        else:
            msg = msg.format(
                e=excp,
                f=parsed_args.youtube_file,
                t=parsed_args.youtube_table,
            )
        parsed_args.logger.error(msg, context_dict=context)
        sys.exit(1)
    msg = "Successfully merged the records in {f} to the table {t}"
    parsed_args.logger.info(msg.format(f=parsed_args.youtube_file, t=parsed_args.youtube_table))


def unknown_command(parsed_args):
    """
    Exit the program if an unknown command is passed (somehow)
    """
    parsed_args.logger.error("Unknown command {c}".format(c=parsed_args.command))
    parsed_args.logger.error("Exiting...")
    sys.exit(1)


def main():
    """
    simeon-youtube entry point
    """
    parser = cli_utils.CustomArgParser(
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter,
        allow_abbrev=False,
        prog="simeon-youtube",
    )
    parser.add_argument(
        "--log-file",
        "-L",
        help="Log file to use when simeon prints messages. Default: stdout",
        type=FileType("a"),
        default=sys.stdout,
    )
    parser.add_argument(
        "--log-format",
        help="Format the log messages as json or text. Default: %(default)s",
        choices=["json", "text"],
        default="json",
    )
    parser.add_argument(
        "--debug",
        "-B",
        help="Show some stacktrace if simeon stops because of a fatal error",
        action="store_true",
    )
    parser.add_argument(
        "--quiet",
        "-Q",
        help="Only print error messages to standard streams.",
        action="store_false",
        dest="verbose",
    )
    parser.add_argument(
        "--config-file",
        "-C",
        help="The INI configuration file to use for default arguments.",
    )
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version="%(prog)s {v}".format(v=simeon.__version__),
    )
    subparsers = parser.add_subparsers(
        description="A subcommand to carry out a task with simeon-youtube",
        dest="command",
    )
    subparsers.required = True
    extractor = subparsers.add_parser(
        "extract",
        help="Extract YouTube video details using the given API token and course axis json.gz files",
        description="Extract YouTube video details using the given API token and course axis json.gz files",
        allow_abbrev=False,
    )
    extractor.add_argument(
        "--output",
        "-o",
        help="The .json.gz output file name for the YouTube video details. Default: %(default)s",
        default=os.path.join(os.getcwd(), "youtube.json.gz"),
    )
    extractor.add_argument(
        "--youtube-token",
        "-t",
        help="YouTube data V3 API token generated from the account hosting the lecture videos.",
    )
    extractor.add_argument(
        "--batch-size",
        "-s",
        help=(
            "How many YouTube video ID's to batch together when making HTTP "
            "requests for video metadata. Default: %(default)s"
        ),
        type=batch_size_type,
        default=10,
    )
    extractor.add_argument(
        "course_axes",
        help="course_axis.json.gz files from the simeon split process of SQL files",
        nargs="+",
    )
    merger = subparsers.add_parser(
        "merge",
        help="Merge the data file generated from simeon-youtube extract to the given target BigQuery table.",
        description=(
            "Merge the data file generated from simeon-youtube extract to the given target BigQuery table."
        ),
        allow_abbrev=False,
    )
    merger.add_argument("youtube_file", help="A .json.gz file generated from the extract command")
    merger.add_argument(
        "--project",
        "-p",
        help="The BigQuery project id where the target table resides.",
    )
    merger.add_argument(
        "--service-account-file",
        "-S",
        help="The service account file to use when connecting to BigQuery",
    )
    merger.add_argument(
        "--target-directory",
        "-T",
        help="A target directory where to export artifacts like compiled SQL queries",
    )
    merger.add_argument(
        "--youtube-table",
        "-y",
        help="The target table where the YouTube video details are stored.",
        default="videos.youtube",
        type=cli_utils.bq_table,
    )
    merger.add_argument(
        "--column",
        "-c",
        help="The column on which to to merge the file and table. Default: %(default)s",
        default="id",
    )
    merger.add_argument(
        "--match-equal-columns",
        help=(
            "Column names for which to set test equality (=) if the WHEN MATCH"
            " SQL condition is met. This is preceded by the AND keyword to "
            "string conditions together."
        ),
        nargs="*",
    )
    merger.add_argument(
        "--match-unequal-columns",
        help=(
            "Column names for which to set test inequality (<>) if the WHEN "
            "MATCH SQL condition is met. This is preceded by the AND keyword to "
            "string conditions together."
        ),
        nargs="*",
    )
    merger.add_argument(
        "--schema-dir",
        "-s",
        help=f"Directory where to find schema files. Default: {SCHEMA_DIR}",
    )
    merger.add_argument(
        "--update-description",
        "-u",
        help=(
            "Update the description of the destination table with "
            'the "description" value from the corresponding schema file'
        ),
        action="store_true",
    )
    args = parser.parse_args()
    args.logger = cli_utils.make_logger(
        user="SIMEON-YOUTUBE:{c}".format(c=args.command.upper()),
        verbose=args.verbose,
        stream=args.log_file,
        json_format=args.log_format == "json",
    )
    try:
        configs = cli_utils.find_config(args.config_file)
    except Exception as excp:
        args.logger.error(str(excp).replace("\n", " "))
        sys.exit(1)
    for k, v in cli_utils.CONFIGS.items():
        for attr, cgetter in v:
            cli_arg = getattr(args, attr, None)
            config_arg = cgetter(configs, k, attr, fallback=None)
            if not cli_arg and config_arg:
                setattr(args, attr, config_arg)
    commands = {
        "extract": extract_video_info,
        "merge": merge_video_data,
    }
    try:
        commands.get(args.command, unknown_command)(args)
    except:
        _, excp, tb = sys.exc_info()
        context = getattr(excp, "context_dict", {})
        if isinstance(excp, SystemExit):
            raise excp
        msg = "The command {c} failed: {e}"
        if args.debug:
            traces = ["{e}".format(e=excp)]
            traces += map(str.strip, traceback.format_tb(tb))
            msg = msg.format(c=args.command, e="\n".join(traces))
        else:
            msg = msg.format(c=args.command, e=excp)
        # msg = 'The command {c} failed with: {e}'
        args.logger.error(msg, context_dict=context)
        sys.exit(1)


if __name__ == "__main__":
    main()
