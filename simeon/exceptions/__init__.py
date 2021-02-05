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
