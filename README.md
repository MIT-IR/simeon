### simeon

`simeon` is a CLI tool to help with the processing of edx Research data. It can `download`, `decrypt`, `split` and `push` edX data packages to GCS and BigQuery. 

### Installing with pip
```sh
python3 -m pip install simeon
# Or with geoip
python3 -m pip install simeon[geoip]
# Then invoke the CLI tool with
simeon --help
```

### Installing with git clone
```sh
git clone git@github.com:MIT-IR/simeon.git
cd simeon && python -m pip install .
# Or with geoip
cd simeon && python -m pip install .[geoip]
# Then invoke the CLI tool with
simeon --help
```


### Developing
```sh
git clone git@github.com:MIT-IR/simeon.git
cd simeon
# Set up a virtual environment if you don't already have on
python3 -m venv venv
. venv/bin/activate
# pip install the package in an editable way
python3 -m pip install -e .[test,geoip]
# Invoke the executable
simeon --help
# Run the tests
tox
# Write code and tests and submit PR's
```


### Setups
`simeon` is a glorified downloader and uploader set of scripts. Much of the downloading and uploading that it does makes the assumptions that you have your AWS credentials configured properly and that you've got a service account file for GCP services available on your machine. If the latter is missing, you may have to authenticate to GCP services through the SDK. However, both we and Google recommend you not do that.

Every downloaded file is decrypted either during the download process or while it gets split by the `simeon split` command. So, this tool assumes that you've installed and configured `gpg` to be able to decrypt files from edX.

The following steps may be useful to someone just getting started with the edX data package:

1. Credentials from edX
    - Reach out to edX to get your data czar credentials
    - Configure both AWS and gpg, so your credentials can access the S3 buckets and your `gpg` key can decrypt the files there
2. Setup a GCP project
    - Create a GCP project
    - Setup a BigQuery workspace
    - Create a GCS bucket
    - Create a service account and download the associated file
    - Give the service account Admin Role access to both the BigQuery project and the GCS bucket

If the above steps are carried out successfully, then you should be able to use `simeon` without any issues.

However, if you've taken care of the above steps but are still unable to get `simeon` to work, please open an issue.

### List files
`simeon` can list files on S3 for your organization based on criteria like file type (`sql` or `log` or `email`), time intervals (begin and end dates), and site (`edx` or `edge` or `patches`).

- Examples:

    ```sh
    # List SQL files dumped since 2021-01-01
    simeon list -s edx -o mitx -f sql -b 2021-01-01
    # List email files dumped since 2021-01-01
    simeon list -s edx -o mitx -f email -b 2021-01-01
    # List tracking log files dumped since 2021-01-01
    simeon list -s edx -o mitx -f log -b 2021-01-01
    ```


### Download and split files
`simeon` can download, decrypt and split up files into folders belonging to specific courses.

- Example 1: Download, split and push SQL bundles to both GCS and BigQuery

    ```sh
    # Download a SQL bundle with the date 2021-02-01 in its file name
    simeon download -s edx -o mitx -f sql -b 2021-02-01 -e 2021-02-01 -d data/

    # Download SQL bundles dumped any time since 2021-01-01 and
    # extract the contents for course ID MITx/12.3x/1T2021.
    # Place the place in data/ and the output of the split in data/SQL
    simeon download -s edx -o mitx -c "MITx/12.3x/1T2021" -f sql \
        -b 2021-01-01 -d data -S -D data/SQL/

    # Push to GCS the split up SQL files inside data/SQL/MITx__12_3x__1T2021
    simeon push gcs -f sql -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} \
        -S ${SAFILE} data/SQL/MITx__12_3x__1T2021

    # Push the files to BigQuery and wait for the jobs to finish
    # Using -s or --use-storage tells BigQuery to extract the files
    # to be loaded from Google Cloud Storage.
    # So, use the option when you've already called simeon push gcs
    simeon push bq -w -s -f sql -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} \
        -S ${SAFILE} data/SQL/MITx__12_3x__1T2021
    ```


- Example 2: Download, split and push tracking logs to both GCS and BigQuery

    ```sh
    # Download a tracking log with the date 021-02-01 in its file name
    simeon download -s edx -o mitx -f log -b 2021-02-01 -e 2021-02-01 -d data/

    # Download tracking logs dumped any time since 2021-01-01
    # and extract the contents for course ID MITx/12.3x/1T2021
    # Place the place in data/ and the output of the split in data/TRACKING_LOGS
    simeon download -s edx -o mitx -c "MITx/12.3x/1T2021" -f log \
        -b 2021-01-01 -d data -S -D data/TRACKING_LOGS/

    # Push to GCS the split up tracking log files inside
    # data/TRACKING_LOGS/MITx__12_3x__1T2021
    simeon push gcs -f log -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} \
        -S ${SAFILE} data/TRACKING_LOGS/MITx__12_3x__1T2021

    # Push the files to BigQuery and wait for the jobs to finish
    # Using -s or --use-storage tells BigQuery to extract the files
    # to be loaded from Google Cloud Storage.
    # So, use the option when you've already called simeon push gcs
    simeon push bq -w -s -f log -p ${GCP_PROJECT_ID} -b ${GCS_BUCKET} \
        -S ${SAFILE} data/TRACKING_LOGS/MITx__12_3x__1T2021
    ```


- If you already have downloaded SQL bundles or tracking log files, you can use `simeon split` them up.

### Make secondary/aggregated tables like person_course, forum_person, etc.
`simeon` can generate secondary tables based on already loaded data. Call `simeon report --help` for the expected positional and optional arguments.

- Example: Make `person_course` for course ID `MITx/12.3x/1T2021`

    ```sh
    # Make a person course table for course ID MITx/12.3x/1T2021
    # Provide the -g option to give a geolocation BigQuery table
    # to fill the ip-to-location details in the generated person course table
    COURSE=MITx/12.3x/1T2021
    simeon report -w -g "${GCP_PROJECT_ID}.geocode.geoip" -t "person_course" \
        -p ${GCP_PROJECT_ID} -S ${SAFILE} ${COURSE}
    ```


### Notes:
1. Please note that SQL bundles are quite large when split up, so consider using the `-c` or `--courses` option when invoking `simeon download -S` or `simeon split` to make sure that you limit the splitting to a set of course IDs.
Otherwise, `simeon` may end up failing to complete the split operation due to exhausted system resources (storage to be specific).

2. `simeon download` with file types `log` and `email` will both download and decrypt the files matching the given criteria. If the latter operations are successful, then the encrypted files are deleted by default. This is to make sure that you don't exhaust storage resources. If you wish to keep those files, you can always use the `--keep-encrypted` option that comes with `simeon download` and `simeon split`.
SQL bundles are only downloaded (not decrypted). Their decryption is done during a split operation.

3. Unless there is an unhandled exception (which should be reported as a bug), `simeon` should, by default, print to the standard output both information and errors encountered while processing your files. You can capture those logs in a file by using the global option `--log-file` and providing a destination file for the logs.