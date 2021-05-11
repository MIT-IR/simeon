"""
Module to process email opt-in data from edX
"""
import os
import zipfile

from simeon.download.utilities import decrypt_files


def process_email_file(
    fname, verbose=True, logger=None, timeout=60, keepfiles=False
):
    """
    Email opt-in files are kind of different in that
    they are zip archives inside of which reside GPG encrypted files.

    :type fname: str
    :param fname: Zip archive containing the email opt-in data file
    :type verbose: bool
    :param verbose: Whether to print stuff when decrypting
    :type logger: logging.Logger
    :param logger: A Logger object to print messages with
    :type timeout: int
    :param timeout: Number of seconds to wait for the decryption to finish
    :type keepfiles: bool
    :param keepfiles: Whether to keep the .gpg files after decrypting them
    :rtype: None
    :return: Nothing
    """
    dirname, out = os.path.split(fname)
    out, _ = os.path.splitext(out)
    out = os.path.join(dirname, '{o}.csv.gpg'.format(o=out))
    with zipfile.ZipFile(fname) as zf, open(out, 'wb') as fh:
        for file_ in zf.infolist():
            if file_.filename.endswith('/'):
                continue
            with zf.open(file_) as zfh:
                while True:
                    chunk = zfh.read(10485760)
                    if not chunk:
                        break
                    fh.write(chunk)
            decrypt_files(
                fnames=out, verbose=verbose,
                logger=logger, timeout=timeout
            )
    if not keepfiles:
        try:
            os.remove(out)
        except OSError:
            pass
