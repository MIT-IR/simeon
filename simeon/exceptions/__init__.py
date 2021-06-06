"""
Exception classes for the simeon package
"""
class AWSException(Exception):
    """
    Raised when an S3 resource can't be made
    """
    pass


class DecryptionError(Exception):
    """
    Raised when the GPG decryption process fails.
    """
    pass


class MissingSchemaException(Exception):
    """
    Raised when a schema could not be found for a given BigQuery table name
    """
    pass


class SchemaMismatchException(Exception):
    """
    Raised when a record does not match its corresponding schema
    """
    pass


class BigQueryNameException(Exception):
    """
    Raised when a fully qualified table or dataset name
    can't be created.
    """
    pass


class BadSQLFileException(Exception):
    """
    Raised when a SQL file is not in its expected format
    """
    pass


class MissingFileException(Exception):
    """
    Raised when a necessary file is missing
    """


class MissingQueryFileException(Exception):
    """
    Raised when a report table does not have a query file
    in the given query directory.
    """
    pass


class EarlyExitError(Exception):
    """
    Raised when an early exit is requested by the end user of
    the CLI tool
    """


class LoadJobException(Exception):
    """
    Raised from a BigQuery data load job
    """
    pass


class SplitException(Exception):
    """
    Raised when an issue happens during a split operation
    """
    pass


class SQLQueryException(Exception):
    """
    Raised when calling client.query raises an error
    """
