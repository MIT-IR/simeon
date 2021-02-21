"""
Utility functions for the simeon CLI tool
"""
import logging
import os
import sys
import configparser
from argparse import ArgumentTypeError

from dateutil.parser import parse as dateparse


def parsed_date(datestr: str) -> str:
    """
    Function to parse the --start-date and --end-date
    options of simeon

    :type datestr: str
    :param datestr: A stringified date
    :rtype: str
    :return: A properly formatted date string
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
    :return: A properly formatted GCS bucket name
    """
    if not bucket.startswith('gs://'):
        return 'gs://{b}'.format(b=bucket)
    return bucket


def optional_file(fname: str) -> str:
    """
    Clean up a given a file path if it's not None.
    Also, check that it exists. Otherwise, raise ArgumentTypeError

    :type fname: str
    :param fname: File name from the command line
    :rtype: str
    :return: A properly formatted file name
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
    :return: Returns a Logger object used to print messages
    """
    if stream is None:
        stream = sys.stdout
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


def make_config_file(dest_path=None):
    """
    Create a config file named 'simeon.ini' that will have the expected
    configuration values
    :type dest_path: str
    :param dest_path: path to save the config file to, if blank uses cwd
    """
    if dest_path is None:
        dest_fname = "simeon.ini"
    else:
        dest_fname = os.path.join(dest_path, "simeon.ini")
    config = configparser.ConfigParser()
    config['GoogleCloud'] = {'Project': 'default-project',
                             'Bucket': 'default-bucket',
                             'ServiceAccountFile': '/path/to/credentials.json'}
    config['AmazonWebServices'] = {}
    config['AmazonWebServices']['Credentials'] = "/path/to/credentials.json"
    with open(dest_fname, 'w') as configfile:
        config.write(configfile)


def load_config(fname="simeon.ini"):
    """
    Load the config file and return the configparser object.
    :type fname: str
    :param fname: filename of config file, default "simeon.ini"
    :return: Returns a ConfigParser object that is dictionary-like
    """
    config = configparser.ConfigParser()
    config.read(fname)
    return config


def find_config(fname="simeon.ini"):
    """
    searches common locations for a config file
    :param fname: filename of config file, default "simeon.ini"
    :return: Returns a ConfigParser object that is dictionary-like
    """
    cwd = os.path.join(os.getcwd(), fname)
    home = os.path.expanduser("~/{f}".format(f=fname))
    if os.path.exists(cwd):
        return load_config(cwd)
    elif os.path.exists(home):
        return load_config(home)


def course_listings(courses_str):
    """
    Given a list of white space separated course IDs,
    split it into a list.
    """
    if courses_str is None:
        return None
    return set(c.strip() for c in courses_str.split(' '))
