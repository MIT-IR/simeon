"""
Utility functions for the simeon CLI tool
"""
import glob
import logging
import os
import sys
import configparser
from argparse import ArgumentTypeError

from dateutil.parser import parse as dateparse


CLI_MAIN_EPILOG = """
RETURN CODES:
    simeon returns either 0 or 1, depending on whether an error was encountered.
    If any error is encountered with any of the subcommands, 1 is returned.
    For simeon list and simeon download, if nothing is listed or downloaded, then 1 is returned.
    For simeon split and simeon push, if nothing ends up being split or pushed, then 1 is returned.
    For simeon report, if any error is encountered while running the queries, then 1 is returned.

SETUP and CONFIGURATIONS:
    simeon is a glorified downloader and uploader set of scripts. Much of the downloading and uploading that it does makes the assumptions that you have
    your AWS credentials configured properly and that you've got a service account file for GCP services available on your machine. If the latter is
    missing, you may have to authenticate to GCP services through the SDK. However, both we and Google recommend the use of service accounts.

    Every downloaded file is decrypted either during the download process or while it gets split by the simeon split command. So, this tool assumes that
    you have installed and configured gpg to be able to decrypt files from edX.

    The following steps may be useful to someone just getting started with the edX data package:

    1. Credentials from edX

        o Reach out to edX to get your data czar credentials

        o Configure both AWS and gpg, so your credentials can access the S3 buckets and your gpg key can decrypt the files there

    2. Setup a GCP project

        o Create a GCP project

        o Setup a BigQuery workspace

        o Create a GCS bucket

        o Create a service account and download the associated file

        o Give the service account Admin Role access to both the BigQuery project and the GCS bucket

    If the above steps are carried out successfully, then you should be able to use simeon without any issues.

    However, if you have taken care of the above steps but are still unable to get simeon to work, please open an issue.

    Further, simeon can parse INI formatted configuration files. It, by default, looks for files in the user's home directory, or in the current working
    directory of the running process. The base names that are targeted when config files are looked up are: simeon.cfg or .simeon.cfg or simeon.ini or .simeon.ini.
    You can also provide simeon with a config file by using the global option --config-file or -C, and giving it a path to the file with the corresponding configurations.

    The following is a sample file content:

        # Default section for things like the organization whose data package is processed
        # You can also set a default site as one of the following: edx, edge, patches
        [DEFAULT]
        site = edx
        org = yourorganizationx
        clistings_file = /path/to/file/with/course_ids

        # Section related to Google Cloud (project, bucket, service account)
        [GCP]
        project = your-gcp-project-id
        bucket = your-gcs-bucket
        service_account_file = /path/to/a/service_account_file.json
        wait_for_loads = True
        geo_table = your-gcp-project.geocode_latest.geoip
        youtube_table = your-gcp-project.videos.youtube
        youtube_token = your-YouTube-API-token

        # Section related to the AWS credentials needed to download data from S3
        [AWS]
        aws_cred_file = ~/.aws/credentials
        profile_name = default

    The options in the config file(s) should match the optional arguments of the CLI tool. For instance, the --service-account-file, --project and
    --bucket options can be provided under the GCP section of the config file as service_account_file, project and bucket, respectively. Similarly, the
    --site and --org options can be provided under the DEFAULT section as site and org, respectively.


EXAMPLES:
List files
    simeon can list files on S3 for your organization based on criteria like file type (sql or log or email), time intervals (begin and end dates), and site (edx or edge or patches).
        # List the latest SQL data dump
        simeon list -s edx -o mitx -f sql -L
        # List the latest email data dump
        simeon list -s edx -o mitx -f email -L
        # List the latest tracking log file
        simeon list -s edx -o mitx -f log -L

Download and split files
    simeon can download, decrypt and split up files into folders belonging to specific courses.

    o Example 1: Download, split and push SQL bundles to both GCS and BigQuery

        # Download the latest SQL data dump
        simeon download -s edx -o mitx -f sql -L -d data/

        # Download SQL bundles dumped any time since 2021-01-01 and
        # extract the contents for course ID MITx/12.3x/1T2021.
        # Place the downloaded files in data/ and the output of the split in data/SQL
        simeon download -s edx -o mitx -c "MITx/12.3x/1T2021" -f sql -b 2021-01-01 -d data -S -D data/SQL/

        # Push to GCS the split up SQL files inside data/SQL/MITx__12_3x__1T2021
        simeon push gcs -f sql -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} -S ${SAFILE} data/SQL/MITx__12_3x__1T2021

        # Push the files to BigQuery and wait for the jobs to finish
        # Using -s or --use-storage tells BigQuery to extract the files
        # to be loaded from Google Cloud Storage.
        # So, use the option when you've already called simeon push gcs
        simeon push bq -w -s -f sql -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} -S ${SAFILE} data/SQL/MITx__12_3x__1T2021

    o Example 2: Download, split and push tracking logs to both GCS and BigQuery

        # Download the latest tracking log file
        simeon download -s edx -o mitx -f log -L -d data/

        # Download tracking logs dumped any time since 2021-01-01
        # and extract the contents for course ID MITx/12.3x/1T2021
        # Place the downloaded files in data/ and the output of the split in data/TRACKING_LOGS
        simeon download -s edx -o mitx -c "MITx/12.3x/1T2021" -f log -b 2021-01-01 -d data -S -D data/TRACKING_LOGS/

        # Push to GCS the split up tracking log files inside
        # data/TRACKING_LOGS/MITx__12_3x__1T2021
        simeon push gcs -f log -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} -S ${SAFILE} data/TRACKING_LOGS/MITx__12_3x__1T2021

        # Push the files to BigQuery and wait for the jobs to finish
        # Using -s or --use-storage tells BigQuery to extract the files
        # to be loaded from Google Cloud Storage.
        # So, use the option when you've already called simeon push gcs
        simeon push bq -w -s -f log -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} -S ${SAFILE} data/TRACKING_LOGS/MITx__12_3x__1T2021

    o If you have already downloaded SQL bundles or tracking log files, you can use simeon split them up.

Make secondary/aggregated tables
    simeon can generate secondary tables based on already loaded data. Call simeon report --help for the expected positional and optional arguments.

    o Example: Make person_course for course ID MITx/12.3x/1T2021

        # Make a person course table for course ID MITx/12.3x/1T2021
        # Provide the -g option to give a geolocation BigQuery table
        # to fill the ip-to-location details in the generated person course table
        COURSE=MITx/12.3x/1T2021
        simeon report -w -g "${GCP_PROJECT_ID}.geocode.geoip" -t "person_course" -p ${GCP_PROJECT_ID} -S ${SAFILE} ${COURSE}


NOTES:
1. Please note that SQL bundles are quite large when split up, so consider using the -c or --courses option when invoking simeon download -S or
    simeon split to make sure that you limit the splitting to a set of course IDs. The `--clistings-file` option is an alternative to `--courses`.
    It expects a text file with one course ID per line.
    If those options are not used, simeon may end up failing to complete the split operation
    due to exhausted system resources (storage to be specific).

2. simeon download with file types log and email will both download and decrypt the files matching the given criteria. If the latter operations are
    successful, then the encrypted files are deleted by default. This is to make sure that you don't exhaust storage resources. If you wish to keep
    those files, you can always use the --keep-encrypted option that comes with simeon download and simeon split. SQL bundles are only downloaded (not decrypted).
    Their decryption is done during a split operation.

3. Unless there is an unhandled exception (which should be reported as a bug), simeon should, by default, print to the standard output both information
    and errors encountered while processing your files. You can capture those logs in a file by using the global option --log-file and providing
    a destination file for the logs.

4. When using multi argument options like --tables or --courses, you should try not to place them right before the expected positional arguments.
    This will help the CLI parser not confuse your positional arguments with table names (in the case of --tables) or course IDs (when --courses is used).

5. Splitting tracking logs is a resource intensive process. The routine that splits the logs generates a file for each course ID encountered. If you
    happen to have more course IDs in your logs than the running process can open operating system file descriptors, then simeon will put away records
    it cannot save to disk for a second pass. Putting away the records involves using more memory than normally required. The second pass will only
    require one file descriptor at a time, so it should be safe in terms of file descriptor limits. To help simeon not have to do a second pass, you
    may increase the file descriptor limits of processes from your shell by running something like ulimit -n 2000 before calling simeon split on Unix
    machines. For Windows users, you may have to dig into the Windows Registries for a corresponding setting. This should tell your OS kernel to allow
    OS processes to open up to 2000 file handles.

6. Care must be taken when using simeon split and simeon push to make sure that the number of positional arguments passed does not lead to the
    invoked command exceeding the maximum command-line length allowed for arguments in a command. To avoid errors along those lines, please consider
    passing the positional arguments as UNIX glob patterns. For instance, simeon split --file-type log 'data/TRACKING-LOGS/*/*.log.gz' tells simeon to
    expand the given glob pattern, instead of relying on the shell to do it.
"""
CONFIGS = {
    'DEFAULT': (
        ('site', configparser.ConfigParser.get),
        ('org', configparser.ConfigParser.get),
        ('clistings_file', configparser.ConfigParser.get),
        ('youtube_token', configparser.ConfigParser.get),
        ('file_format', configparser.ConfigParser.get),
        ('schema_dir', configparser.ConfigParser.get),
        ('max_bad_rows', configparser.ConfigParser.getint),
        ('update_description', configparser.ConfigParser.getboolean),
        ('schema_dir', configparser.ConfigParser.get),
        ('query_dir', configparser.ConfigParser.get),
        ('project', configparser.ConfigParser.get),
        ('bucket', configparser.ConfigParser.get),
        ('service_account_file', configparser.ConfigParser.get),
        ('geo_table', configparser.ConfigParser.get),
        ('youtube_table', configparser.ConfigParser.get),
    ),
    'GCP': (
        ('project', configparser.ConfigParser.get),
        ('bucket', configparser.ConfigParser.get),
        ('service_account_file', configparser.ConfigParser.get),
        ('wait_for_loads', configparser.ConfigParser.getboolean),
        ('use_storage', configparser.ConfigParser.getboolean),
        ('geo_table', configparser.ConfigParser.get),
        ('youtube_table', configparser.ConfigParser.get),
        ('youtube_token', configparser.ConfigParser.get),
        ('file_format', configparser.ConfigParser.get),
        ('schema_dir', configparser.ConfigParser.get),
        ('query_dir', configparser.ConfigParser.get),
        ('max_bad_rows', configparser.ConfigParser.getint),
    ),
    'AWS': (
        ('aws_cred_file', configparser.ConfigParser.get),
        ('profile_name', configparser.ConfigParser.get),
    ),
}
REPORT_TABLES = [
    'video_axis', 'forum_events', 'problem_grades', 'chapter_grades',
    'show_answer', 'video_stats_day', 'show_answer_stats_by_user',
    'show_answer_stats_by_course', 'course_item', 'person_item',
    'person_problem', 'course_problem', 'person_course_day',
    'pc_video_watched', 'pc_day_totals', 'pc_day_trlang',
    'pc_day_ip_counts', 'language_multi_transcripts', 'pc_nchapters',
    'pc_forum', 'course_modal_language', 'course_modal_ip',
    'forum_posts', 'forum_person', 'enrollment_events', 'enrollday_all',
    'person_enrollment_verified', 'person_course',
]


def parsed_date(datestr: str) -> str:
    """
    Function to parse the --start-date and --end-date
    options of simeon

    :type datestr: str
    :param datestr: A stringified date
    :rtype: str
    :returns: A properly formatted date string
    :raises: ArgumentTypeError
    """
    try:
        return dateparse(datestr).strftime('%Y-%m-%d')
    except Exception:
        msg = '{d!r} could not be parsed into a proper date'
        raise ArgumentTypeError(msg.format(d=datestr))


def gcs_bucket(bucket: str) -> str:
    """
    Clean up a GCS bucket name if it does not start with the gs:// protocol

    :type bucket: str
    :param bucket: Google Cloud Storage bucket name
    :rtype: str
    :returns: A properly formatted GCS bucket name
    """
    if not bucket.startswith('gs://'):
        return 'gs://{b}'.format(b=bucket)
    return bucket


def bq_table(name):
    """
    Check that the given BigQuery table name
    is a fully qualified one: dataset.table or project.dataset.table
    """
    if not name:
        return None
    chunks = name.split('.')
    if len(chunks) not in (2, 3):
        raise ArgumentTypeError(
            '{n} is not a valid BigQuery table name.\nValid table names '
            'should be in the form project.dataset.table '
            'or dataset.table.'.format(n=name)
        )
    return name


def optional_file(fname: str) -> str:
    """
    Clean up a given a file path if it's not None.
    Also, check that it exists. Otherwise, raise ArgumentTypeError

    :type fname: str
    :param fname: File name from the command line
    :rtype: str
    :returns: A properly formatted file name
    :raises: ArgumentTypeError
    """
    if fname is not None:
        fname = os.path.expanduser(fname)
        if not os.path.exists(fname):
            msg = 'The given file name {f!r} does not exist.'
            raise ArgumentTypeError(msg.format(f=fname))
    return os.path.realpath(fname)


def make_logger(user='SIMEON', verbose=True, stream=None):
    """
    Create a Logger object pointing to the given stream

    :type verbose: bool
    :param verbose: If True, log level is INFO. Otherwise, it's WARN
    :type stream: Union[TextIOWrapper,None]
    :param stream: A file object opened for writing
    :rtype: logging.Logger
    :returns: Returns a Logger object used to print messages
    """
    if stream is None:
        stream = sys.stdout
    if not hasattr(stream, 'write'):
        stream = open(stream, 'a')
    level = logging.INFO if verbose else logging.WARN
    formatter = logging.Formatter(
        '%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        '%Y-%m-%d %H:%M:%S%z'
    )
    handler = logging.StreamHandler(stream=stream)
    handler.setLevel(level)
    handler.set_name(user)
    handler.setFormatter(formatter)
    logger = logging.Logger(user, level)
    logger.addHandler(handler)
    return logger


def make_config_file(output=None):
    """
    Create a config file named 'simeon.cfg' that will have the expected
    configuration values

    :type output: Union[None, str, pathlib.Path]
    :param output: Path to the config file where sections and options are put
    :rtype: None
    :returns: Write the config info to the given output file
    """
    if output is None:
        output = os.path.join(os.path.expanduser('~'), 'simeon.cfg')
    config = configparser.ConfigParser()
    config.optionxform = lambda s: s.lstrip('-').lower().replace('-', '_')
    config['DEFAULT'] = {
        'site': '',
        'org': '',
    }
    config['GCP'] = {
        'project': '',
        'bucket': '',
        'service_account_file': '',
    }
    config['AWS'] = {
        'credential_file': '',
        'profile_name': '',
    }
    with open(output, 'w') as configfile:
        config.write(configfile)


def find_config(fname=None, no_raise=False):
    """
    Searches for config files in default locations.
    If no file name is provided, it tries to load files
    in the home and current directories of the running process.

    :type fname: Union[None, str, pathlib.Path]
    :param fname: Path to an INI config file, default "simeon.cfg"
    :rtype: configparser.ConfigParser
    :returns: Returns a ConfigParser with configs from the file(s)
    """
    if fname is None:
        names = ('simeon.cfg', 'simeon.ini', '.simeon.cfg', '.simeon.ini')
        files = []
        for name in names:
            files.extend([
                os.path.join(os.path.expanduser('~'), name),
                os.path.join(os.getcwd(), name)
            ])
    else:
        files = [os.path.expanduser(fname)]
    config = configparser.ConfigParser()
    config.optionxform = lambda s: s.lstrip('-').lower().replace('-', '_')
    for config_file in files:
        try:
            config.read(config_file)
        except Exception as excp:
            if no_raise:
                continue
            raise ArgumentTypeError(excp) from None
    return config


def course_listings(courses):
    """
    Given a list of white space separated course IDs,
    split it into a list.

    :type courses: Union[Iterable, None]
    :param courses: An iterable of course ID's
    :rtype: set
    :returns: A set object of course IDs
    """
    if courses is None:
        return None
    return set(c.strip() for c in courses if c.strip())


def courses_from_file(fname):
    """
    Given the path to a file, extract the course IDs in it.
    One course ID per line.

    :type fname: str
    :param fname: Path to a file with course listings. 1 course ID per line
    :rtype: Union[None, set]
    :returns: A set of course IDs
    """
    if not fname:
        return None
    fname = os.path.expanduser(fname)
    if not os.path.isfile(fname):
        msg = 'The given course listings file, {f!r}, is not a valid file'
        raise ArgumentTypeError(msg.format(f=fname))
    with open(fname) as fh:
        return set(line.strip() for line in fh if line.strip())


def course_paths_from_file(fname):
    """
    Given the path to a file, extract the course IDs in it, and
    turn the course IDs into matching directory names.

    :type fname: str
    :param fname: Path to a file with course listings. 1 course ID per line
    :rtype: Union[None, set]
    :returns: A set of directory names
    """
    if not fname:
        return None
    fname = os.path.expanduser(fname)
    if not os.path.isfile(fname):
        msg = 'The given course listings file, {f!r}, is not a valid file'
        raise ArgumentTypeError(msg.format(f=fname))
    out = set()
    with open(fname) as fh:
        for line in fh:
            c = line.strip().replace('/', '__').replace('+', '__')
            out.add(c.replace('.', '_').replace('-', '_').lower())
        return out


def filter_generated_items(items, cdirs):
    """
    This is used to filter the given items (which are file paths)
    using the provided course ID directories. Since the directories
    are generated by the course_paths_from_file function, the items
    in cdirs are expected to lowercased.

    :type items: Union[List[str], Set[str]]
    :param items: Items passed to the simeon push command
    :type cdirs: Set[str]
    :param cdirs: Course ID directories (lowercased)
    :rtype: Set[str]
    :return: Returns a set of file paths whose directories are in cdirs
    """
    if not cdirs:
        return set(items)
    out = set()
    for item in items:
        if '*' not in item:
            if os.path.isdir(item):
                real_item = os.path.basename(os.path.realpath(item))
            else:
                real_item = os.path.basename(
                    os.path.dirname(os.path.realpath(item))
                )
            if real_item.lower() in cdirs:
                out.add(item)
        else:
            out.update(filter_generated_items(glob.glob(item), cdirs))
    return out


def items_from_files(files):
    """
    Given a list of text files, extract file paths
    """
    out = set()
    for file_ in files:
        with open(file_) as fh:
            out.update(map(str.strip, fh))
    return out


def expand_paths(items):
    """
    Expand glob patterns in items
    """
    out = []
    for path in items:
        out.extend(glob.iglob(path))
    return out


def is_parallel(args):
    """
    Take a parsed argparse.Namespace and check that simeon
    will be using some paraellism.
    It also sets an is_parallel on the Namespace object
    """
    if getattr(args, 'command', '') not in ('download', 'split', 'report'):
        args.is_parallel = False
    elif getattr(args, 'dynamic_date', False):
        args.is_parallel = False
    elif hasattr(args, 'in_files'):
        args.is_parallel = True
    else:
        items = getattr(
            args, 'downloaded_files', getattr(args, 'course_ids', [])
        )
        args.is_parallel = len(expand_paths(items)) > 1
    return args.is_parallel
