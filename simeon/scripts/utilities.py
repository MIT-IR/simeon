"""
Utility functions for the simeon CLI tool
"""
import logging
import os
import sys
import configparser
from argparse import ArgumentTypeError

from dateutil.parser import parse as dateparse

CONFIGS = {
    'DEFAULT': (
        ('site', configparser.ConfigParser.get),
        ('org', configparser.ConfigParser.get),
        ('clistings_file', configparser.ConfigParser.get),
    ),
    'GCP': (
        ('project', configparser.ConfigParser.get),
        ('bucket', configparser.ConfigParser.get),
        ('service_account_file', configparser.ConfigParser.get),
        ('wait_for_loads', configparser.ConfigParser.getboolean),
        ('use_storage', configparser.ConfigParser.getboolean),
        ('geo_table', configparser.ConfigParser.get),
    ),
    'AWS': (
        ('credential_file', configparser.ConfigParser.get),
        ('profile_name', configparser.ConfigParser.get),
    ),
}


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
        stream = open(stream, 'w')
    level = logging.INFO if verbose else logging.WARN
    formatter = logging.Formatter(
        '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
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


def find_config(fname=None):
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
        files = [
            os.path.join(os.path.expanduser('~'), 'simeon.cfg'),
            os.path.join(os.path.expanduser('~'), '.simeon.cfg'),
            os.path.join(os.path.expanduser('~'), 'simeon.ini'),
            os.path.join(os.path.expanduser('~'), '.simeon.ini'),
            os.path.join(os.path.join(os.getcwd(), 'simeon.cfg')),
            os.path.join(os.path.join(os.getcwd(), '.simeon.cfg')),
            os.path.join(os.path.join(os.getcwd(), 'simeon.ini')),
            os.path.join(os.path.join(os.getcwd(), '.simeon.ini')),
        ]
    else:
        files = [fname]
    config = configparser.ConfigParser()
    for config_file in files:
        config.read(config_file)
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
    return set(c.strip() for c in courses)


def course_from_file(fname):
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
        return set(line.strip() for line in fh)
