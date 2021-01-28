"""
Utility functions for the simeon CLI tool
"""
import logging
import os
import sys
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


def make_logger(verbose=True, stream=None):
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
    handler.set_name('SIMEON')
    handler.setFormatter(formatter)
    logger = logging.Logger('SIMEON', level)
    logger.addHandler(handler)
    return logger
