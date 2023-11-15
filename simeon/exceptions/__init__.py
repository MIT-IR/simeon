"""
Exception classes for the simeon package
"""


class SimeonError(Exception):
    """
    Base exception for most simeon issues
    """

    def __init__(self, message, context_dict=None):
        super().__init__(message)
        self.context_dict = context_dict or {}


class AWSException(Exception):
    """
    Raised when an S3 resource can't be made
    """

    pass


class BlobDownloadError(AWSException):
    """
    Raised when a Blob fails to download. This could be an upstream issue.
    However, it can also be due to local file system access issues, or due
    to exhausted system resources
    """


class DecryptionError(SimeonError):
    """
    Raised when the GPG decryption process fails.
    """

    pass


class MissingSchemaException(SimeonError):
    """
    Raised when a schema could not be found for a given BigQuery table name
    """

    pass


class SchemaMismatchException(SimeonError):
    """
    Raised when a record does not match its corresponding schema
    """

    pass


class BigQueryNameException(SimeonError):
    """
    Raised when a fully qualified table or dataset name
    can't be created.
    """

    pass


class BadSQLFileException(SimeonError):
    """
    Raised when a SQL file is not in its expected format
    """

    pass


class MissingFileException(SimeonError):
    """
    Raised when a necessary file is missing
    """


class MissingQueryFileException(SimeonError):
    """
    Raised when a report table does not have a query file
    in the given query directory.
    """

    pass


class EarlyExitError(SimeonError):
    """
    Raised when an early exit is requested by the end user of
    the CLI tool
    """


class LoadJobException(SimeonError):
    """
    Raised from a BigQuery data load job
    """

    pass


class SplitException(SimeonError):
    """
    Raised when an issue happens during a split operation
    """

    pass


class SQLQueryException(SimeonError):
    """
    Raised when calling client.query raises an error
    """
