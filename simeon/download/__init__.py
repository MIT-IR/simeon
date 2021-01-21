"""
Utility functions and classes to help with downloading and decrypting
the data from S3.
"""
from .aws import (
    S3Blob, decrypt_file, make_s3_bucket, process_email_file
)