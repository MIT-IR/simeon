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
