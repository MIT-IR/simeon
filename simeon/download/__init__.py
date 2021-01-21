"""
Utility functions and classes to help process edX Research data
"""
from .aws import (
    S3Blob, decrypt_file, make_s3_bucket, process_email_file
)