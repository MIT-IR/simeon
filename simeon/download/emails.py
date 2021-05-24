"""
Module to process email opt-in data from edX
"""
import csv
import gzip
import json
import os
import zipfile

from dateutil.parser import parse as parse_date

from simeon.download.utilities import decrypt_files


SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'upload', 'schemas'
)


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
    :rtype: str
    :return: Returns the path to the decrypted file
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
    return os.path.splitext(out)[0]


def compress_email_files(files, ddir, schema_dir=SCHEMA_DIR):
    """
    Generate a GZIP JSON file in the given ddir directory
    using the contents of the files.

    :NOTE: schema_dir is not used yet. But we may use to check that
    the generated records match their destination tables.

    :type files: Iterable[str]
    :param files: An iterable of email opt-in CSV files to process
    :type ddir: str
    :param ddir: A destination directory
    :type schema_dir: str
    :param schema_dir: Directory where schema files live
    :rtype: None
    :return: Writes the contents of files into email_opt_in.json.gz
    """
    outname = os.path.join(ddir, 'email_opt_in.json.gz')
    with gzip.open(outname, 'wt') as fh:
        for file_ in files:
            with open(file_) as infile:
                cols = [c.strip() for c in next(infile).split(',')]
                reader = csv.DictReader(
                    infile, delimiter=',',
                    lineterminator='\n', fieldnames=cols
                )
                for row in reader:
                    cid = (row.get('course_id') or '').split(':')[-1]
                    row['course_id'] = cid.replace(
                        '+', '/', 2
                    ).replace('+', '_')
                    row['preference_set_datetime'] = parse_date(
                        row.get('preference_set_datetime')
                    ).isoformat()
                    is_opt = (row.get('is_opted_in_for_email') or '').strip()
                    row['is_opted_in_for_email'] = is_opt.lower() == 'true'
                    fh.write(json.dumps(row) + '\n')
