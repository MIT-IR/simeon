"""
Utility functions for the simeon CLI tool
"""
import argparse
import configparser
import datetime
import glob
import io
import json
import logging
import os
import shlex
import socket
import stat
import subprocess as sb
import sys
import typing
from argparse import ArgumentTypeError

from dateutil.parser import parse as dateparse


CONFIGS = {
    "DEFAULT": (
        ("site", configparser.ConfigParser.get),
        ("org", configparser.ConfigParser.get),
        ("clistings_file", configparser.ConfigParser.get),
        ("youtube_token", configparser.ConfigParser.get),
        ("file_format", configparser.ConfigParser.get),
        ("schema_dir", configparser.ConfigParser.get),
        ("max_bad_rows", configparser.ConfigParser.getint),
        ("update_description", configparser.ConfigParser.getboolean),
        ("schema_dir", configparser.ConfigParser.get),
        ("query_dir", configparser.ConfigParser.get),
        ("project", configparser.ConfigParser.get),
        ("bucket", configparser.ConfigParser.get),
        ("service_account_file", configparser.ConfigParser.get),
        ("geo_table", configparser.ConfigParser.get),
        ("youtube_table", configparser.ConfigParser.get),
        ("extra_args", configparser.ConfigParser.get),
        ("email_bucket", configparser.ConfigParser.get),
        ("sql_bucket", configparser.ConfigParser.get),
        ("log_bucket", configparser.ConfigParser.get),
        ("rdx_bucket", configparser.ConfigParser.get),
        ("target_directory", configparser.ConfigParser.get),
    ),
    "GCP": (
        ("project", configparser.ConfigParser.get),
        ("bucket", configparser.ConfigParser.get),
        ("service_account_file", configparser.ConfigParser.get),
        ("wait_for_loads", configparser.ConfigParser.getboolean),
        ("use_storage", configparser.ConfigParser.getboolean),
        ("geo_table", configparser.ConfigParser.get),
        ("youtube_table", configparser.ConfigParser.get),
        ("youtube_token", configparser.ConfigParser.get),
        ("file_format", configparser.ConfigParser.get),
        ("schema_dir", configparser.ConfigParser.get),
        ("query_dir", configparser.ConfigParser.get),
        ("max_bad_rows", configparser.ConfigParser.getint),
        ("extra_args", configparser.ConfigParser.get),
        ("target_directory", configparser.ConfigParser.get),
    ),
    "AWS": (
        ("aws_cred_file", configparser.ConfigParser.get),
        ("profile_name", configparser.ConfigParser.get),
        ("email_bucket", configparser.ConfigParser.get),
        ("sql_bucket", configparser.ConfigParser.get),
        ("log_bucket", configparser.ConfigParser.get),
        ("rdx_bucket", configparser.ConfigParser.get),
    ),
}
REPORT_TABLES = [
    "video_axis",
    "forum_events",
    "problem_grades",
    "chapter_grades",
    "show_answer",
    "video_stats_day",
    "show_answer_stats_by_user",
    "show_answer_stats_by_course",
    "course_item",
    "person_item",
    "person_problem",
    "course_problem",
    "person_course_day",
    "pc_video_watched",
    "pc_day_totals",
    "pc_day_trlang",
    "pc_day_ip_counts",
    "language_multi_transcripts",
    "pc_nchapters",
    "pc_forum",
    "course_modal_language",
    "course_modal_ip",
    "forum_posts",
    "forum_person",
    "enrollment_events",
    "enrollday_all",
    "person_enrollment_verified",
    "pc_day_agent_counts",
    "course_modal_agent",
    "person_course",
]
EXTRA_ARG_TYPE = {
    "i": int,
    "f": float,
    "s": str,
}


def parsed_date(datestr: typing.Union[str, datetime.datetime, datetime.date]) -> str:
    """
    Function to parse the --begin-date and --end-date
    options of simeon

    :type datestr: typing.Union[str, datetime.datetime, datetime.date]
    :param datestr: A stringified date or a date/datetime object
    :rtype: str
    :returns: A properly formatted date string
    :raises: ArgumentTypeError
    """
    if isinstance(datestr, (datetime.date, datetime.datetime)):
        return datestr.strftime("%Y-%m-%d")
    try:
        return dateparse(datestr).strftime("%Y-%m-%d")
    except Exception:
        msg = "{d!r} could not be parsed into a proper date"
        raise ArgumentTypeError(msg.format(d=datestr))


def gcs_bucket(bucket: str) -> str:
    """
    Clean up a GCS bucket name if it does not start with the gs:// protocol

    :type bucket: str
    :param bucket: Google Cloud Storage bucket name
    :rtype: str
    :returns: A properly formatted GCS bucket name
    """
    if not bucket.startswith("gs://"):
        return "gs://{b}".format(b=bucket)
    return bucket


def bq_table(name):
    """
    Check that the given BigQuery table name
    is a fully qualified one: dataset.table or project.dataset.table
    """
    if not name:
        return None
    chunks = name.split(".")
    if len(chunks) not in (2, 3):
        raise ArgumentTypeError(
            "{n} is not a valid BigQuery table name.\nValid table names "
            "should be in the form project.dataset.table "
            "or dataset.table.".format(n=name)
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
            msg = "The given file name {f!r} does not exist."
            raise ArgumentTypeError(msg.format(f=fname))
    return os.path.realpath(fname)


def make_logger(user="SIMEON", verbose=True, stream=None, json_format=True):
    """
    Create a Logger object pointing to the given stream

    :type user: str
    :param user: User running the process
    :type verbose: bool
    :param verbose: If True, log level is INFO. Otherwise, it's WARN
    :type stream: Union[TextIOWrapper,None]
    :param stream: A file object opened for writing
    :type json_format: bool
    :param json_format: Whether or not to show log messages as JSON
    :rtype: Union[TextLoggerAdapter, JSONLoggerAdapter]
    :returns: Returns a Logger object used to print messages
    """
    if stream is None:
        stream = sys.stdout
    if not hasattr(stream, "write"):
        stream = open(stream, "a")
    level = logging.INFO if verbose else logging.WARN
    formatter = logging.Formatter(
        "%(asctime)s:%(hostname)s:%(levelname)s:%(name)s:%(message)s",
        "%Y-%m-%d %H:%M:%S%z",
    )
    logger = logging.getLogger(user.upper())
    logger.setLevel(level)
    handler = logging.StreamHandler(stream=stream)
    handler.setLevel(level)
    handler.set_name(user)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    adapter_class = JSONLoggerAdapter if json_format else TextLoggerAdapter
    return adapter_class(logger, {"hostname": socket.gethostname()})


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
        output = os.path.join(os.path.expanduser("~"), "simeon.cfg")
    config = configparser.ConfigParser()
    config.optionxform = lambda s: s.lstrip("-").lower().replace("-", "_")
    seen = set()
    # The order of the sections matters. We should not duplicate configs in DEFAULT.
    for section in ["AWS", "GCP", "DEFAULT"]:
        details = CONFIGS.get(section)
        section_info = dict()
        for detail in details:
            name, *_ = detail
            if name in seen:
                continue
            seen.add(name)
            section_info[name] = ""
        config[section] = section_info
    with open(output, "w") as configfile:
        config.write(configfile)


def find_config(fname=None, no_raise=False):
    """
    Searches for config files in default locations.
    If no file name is provided, it tries to load files
    in the home and current directories of the running process.

    :type fname: Union[None, str, pathlib.Path]
    :param fname: Path to an INI config file, default "simeon.cfg"
    :type no_raise: bool
    :param no_raise: Whether to raise an error when the config file fails to parse.
    :rtype: configparser.ConfigParser
    :returns: Returns a ConfigParser with configs from the file(s)
    """
    if fname is None:
        possible_names = ("simeon.cfg", "simeon.ini", ".simeon.cfg", ".simeon.ini")
        files = []
        for name in possible_names:
            files.extend([os.path.join(os.path.expanduser("~"), name), os.path.join(os.getcwd(), name)])
    else:
        files = [os.path.expanduser(fname)]
    config = configparser.ConfigParser()
    config.optionxform = lambda s: s.lstrip("-").lower().replace("-", "_")
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
    out = set()
    for c in courses:
        out.add(c.strip().split(":")[-1].replace("+", "/"))
    return out


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
        msg = "The given course listings file, {f!r}, is not a valid file"
        raise ArgumentTypeError(msg.format(f=fname))
    out = set()
    with open(fname) as fh:
        for c in fh:
            out.add(c.strip().split(":")[-1].replace("+", "/"))
    return out


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
        msg = "The given course listings file, {f!r}, is not a valid file"
        raise ArgumentTypeError(msg.format(f=fname))
    out = set()
    with open(fname) as fh:
        for line in fh:
            line = line.split(":")[-1]
            c = line.strip().replace("/", "__").replace("+", "__")
            out.add(c.replace(".", "_").replace("-", "_").lower())
        return out


def filter_generated_items(items, cdirs):
    """
    This is used to filter the given items (which are file paths)
    using the provided course ID directories. Since the directories
    are generated by the course_paths_from_file function, the items
    in cdirs are expected to be lowercase.

    :type items: Union[List[str], Set[str]]
    :param items: Items passed to the simeon push command
    :type cdirs: Set[str]
    :param cdirs: Course ID directories (lowercase)
    :rtype: Set[str]
    :return: Returns a set of file paths whose directories are in cdirs
    """
    if not cdirs:
        return set(items)
    out = set()
    for item in items:
        # If the item is not a glob pattern, or the expansion of the pattern
        # yields a list that contains the item itself, then we are dealing
        # with a path that just happens to have an asterisk in its name.
        # Either way, we'll treat item as a normal path
        if "*" not in item or item in glob.glob(item):
            if os.path.isdir(item):
                real_item = os.path.basename(os.path.realpath(item))
            else:
                real_item = os.path.basename(os.path.dirname(os.path.realpath(item)))
            if real_item.lower() in cdirs:
                out.add(item)
        else:
            # If we're dealing with an actual glob pattern, then recursively
            # process its matches from glob.glob
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
    will be using some parallelism.
    It also sets an is_parallel on the Namespace object
    """
    if getattr(args, "command", "") not in ("download", "split", "report"):
        args.is_parallel = False
    elif getattr(args, "dynamic_date", False):
        args.is_parallel = False
    elif hasattr(args, "in_files"):
        args.is_parallel = True
    else:
        items = getattr(args, "downloaded_files", getattr(args, "course_ids", []))
        args.is_parallel = len(expand_paths(items)) > 1
    return args.is_parallel


def process_extra_args(extras):
    """
    Take a string in the form "var1=val1,var2,val2,...,var_n=val_n" and convert
    it to a dict

    :type extras: Union[dict, str]
    :param extras: String in the form "var1=val1,var2,val2,...,var_n=val_n"
    :rtype: dict
    :return: Returns a dict from the string: var for key and val for value
    """
    # If we nothing, return an empty dictionary
    if not extras:
        return {}

    # If we already have a dict object, then give it back to the user.
    if isinstance(extras, dict):
        return extras

    # If it's a file, try reading it as a JSON file
    if isinstance(extras, str) and extras.startswith("@"):
        with open(extras) as file_handle:
            return json.load(file_handle)

    # Otherwise, process the string
    out = dict()
    types = EXTRA_ARG_TYPE
    extras = map(lambda p: p.lstrip().split("=")[:2], extras.split(","))
    while True:
        try:
            k, v = next(extras)
            chunks = iter(k.split(":"))
            k = next(chunks)
            type_ = next(chunks, None)
            func = types.get(type_) or str
            v = func(v)
            if k in out:
                if not isinstance(out[k], list):
                    out[k] = [out[k]]
                out[k].append(v)
            else:
                out[k] = v
        except StopIteration:
            break
        except ValueError as excp:
            msg = "The provided extra arguments are not properly formatted: {e}"
            raise Exception(msg.format(e=excp)) from None
    return out


class NumberRange(object):
    """
    Check if a number is within a range. Otherwise, raise ArgumentTypeError
    """

    def __init__(self, type_=int, lower=1, upper=50):
        self.type_ = type_
        self.lower = lower
        self.upper = upper

    def __call__(self, value):
        try:
            v = self.type_(value)
        except Exception as excp:
            msg = "The given value {v} could not be converted to a number. Please provide a valid value: {e}."
            raise ArgumentTypeError(msg.format(v=value, e=excp)) from None
        if not (self.lower <= v <= self.upper):
            msg = "{v} is not in the range [{l}, {u}]"
            raise ArgumentTypeError(msg.format(v=value, l=self.lower, u=self.upper))
        return v


class TableOrderAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if any(k in sys.argv for k in ("--no-reorder", "-O")):
            print(values)
            setattr(namespace, "tables", values)
            return
        tables = []
        for table in REPORT_TABLES:
            if table in values:
                tables.append(table)
        if not tables:
            if not values:
                raise ArgumentTypeError("Table names required when --tables is given")
            setattr(namespace, "tables", values)
            return
        for table in values:
            if table not in tables:
                tables.append(table)
        setattr(namespace, "tables", tables)


class CustomArgParser(argparse.ArgumentParser):
    """
    A custom ArgumentParser class that prints help messages
    using either less or more, if available. Otherwise, it does
    what ArgumentParser does.
    """

    @staticmethod
    def _is_executable(f):
        try:
            md = os.stat(f).st_mode
            return bool(md & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))
        except OSError:
            return False

    def get_pager(self):
        """
        Get path to less or more, or any other pager provided by the system via the PAGER environment variable.

        :rtype: tuple[str, str]
        :return: Path to the executable to use as a pager, along with any arguments
        """
        pagers = (
            os.getenv("PAGER"),
            "less",
            "more",
        )
        for path in (os.getenv("PATH") or "").split(os.path.pathsep):
            for pager in pagers:
                if pager is None:
                    continue
                # If the pager is already the full path to the executable, then return it.
                if self._is_executable(pager):
                    return pager, ""
                # Otherwise, handle the cases where it's only the program and cases where the program with arguments.
                pager = iter(pager.split(" ", 1))
                prog = os.path.join(path, next(pager))
                args = next(pager, None) or ""
                if self._is_executable(prog):
                    return prog.strip(), args.strip()
                else:
                    continue
        return "", ""

    def print_help(self, file=None):
        pager_prog, pager_args = self.get_pager()
        if not pager_prog:
            return super().print_help(file)
        with io.StringIO() as fh:
            super().print_help(fh)
            # Have the pager (ideally less or more) read from its standard input.
            cmd = shlex.split(f"{pager_args or ''} -".strip())
            with sb.Popen(cmd, stdin=sb.PIPE, executable=pager_prog, text=True) as proc:
                # Send the contents of the help via stdin and ignore any broken pipe errors
                try:
                    fh.seek(0, 0)
                    proc.stdin.write(fh.read())
                except BrokenPipeError:
                    pass
                finally:
                    proc.stdin.close()
                # Wait for the pager to exit.
                rc = proc.wait()
                # If the pager didn't open successfully, use the default print_help implementation.
                if rc != 0:
                    return super().print_help(file)


class JSONLoggerAdapter(logging.LoggerAdapter):
    """
    A LoggerAdapter that converts the message into a JSON record.
    This finds a dict called context_dict in the passed-in keywords
    arguments and uses that as a starting dict to eventually convert into
    a JSON string.
    As a result, every invocation of info, debug, error, fatal, warning
    should pass the context_dict keyword argument if additional details
    are to be passed to the JSON string.
    """

    def process(self, msg, kwargs):
        context_dict = kwargs.pop("context_dict", {})
        msg, kwargs = super().process(msg, kwargs)
        context_dict.update(self.extra)
        context_dict["message"] = msg
        return json.dumps(context_dict, default=str), kwargs


class TextLoggerAdapter(logging.LoggerAdapter):
    """
    This is basically defined to pop the context_dict dictionary
    from the kwargs dict passed to logging.LoggerAdapter.process
    """

    def process(self, msg, kwargs):
        kwargs.pop("context_dict", None)
        return super().process(msg, kwargs)


def get_main_epilog():
    """
    Get the epilog text for the simeon CLI
    """
    fname = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", "simeon_epilog.txt")
    try:
        return open(fname).read()
    except:
        return ""


__all__ = [
    "CustomArgParser",
    "JSONLoggerAdapter",
    "NumberRange",
    "TableOrderAction",
    "TextLoggerAdapter",
    "bq_table",
    "course_listings",
    "course_paths_from_file",
    "courses_from_file",
    "expand_paths",
    "filter_generated_items",
    "find_config",
    "gcs_bucket",
    "get_main_epilog",
    "is_parallel",
    "items_from_files",
    "make_config_file",
    "make_logger",
    "optional_file",
    "parsed_date",
    "process_extra_args",
]
