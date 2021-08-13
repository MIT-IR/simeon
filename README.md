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

### Using Docker
```sh
docker run -it mitir/simeon:latest
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


### Setups and configurations
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

Further, `simeon` can parse INI formatted configuration files. It, by default, looks for files in the user's home directory, or in the current working directory of the running process. The base names that are targeted when config files are looked up are: `simeon.cfg` or `.simeon.cfg` or `simeon.ini` or `.simeon.ini`. You can also provide `simeon` with a config file by using the global option `--config-file` or `-C` and giving it a path to the file with the corresponding configurations.

The following is a sample file content:

```sh
# Default section for things like the organization whose data package is processed
# You can also set a default site as one of the following: edx, edge, patches
[DEFAULT]
site = edx
org = yourorganizationx
clistings_file = /path/to/file/with/course_ids

# Section related to Google Cloud (project, bucket, service account)
[GCP]
project = your-gcp-project-id
bucket = your-gcs-bucket
service_account_file = /path/to/a/service_account_file.json
wait_for_loads = True
geo_table = your-gcp-project.geocode_latest.geoip
youtube_table = your-gcp-project.videos.youtube
youtube_token = your-YouTube-API-token

# Section related to the AWS credentials needed to download data from S3
[AWS]
aws_cred_file = ~/.aws/credentials
profile_name = default
```

The options in the config file(s) should match the optional arguments of the CLI tool. For instance, the `--service-account-file`, `--project` and `--bucket` options can be provided under the `GCP` section of the config file as `service_account_file`, `project` and `bucket`, respectively. Similarly, the `--site` and `--org` options can be provided under the `DEFAULT` section as `site` and `org`, respectively.


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

### Make secondary/aggregated tables
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


4. When using multi argument options like `--tables` or `--courses`, you should try not to place them right before the expected positional arguments. This will help the CLI parser not confuse your positional arguments with table names (in the case of `--tables`) or course IDs (when `--courses` is used).


5. Splitting tracking logs is a resource intensive process. The routine that splits the logs generates a file for each course ID encountered. If you happen to have more course IDs in your logs than the running process can open operating system file descriptors, then `simeon` will put away records it can't save to disk for a second pass. Putting away the records involves using more memory than normally required. The second pass will only require one file descriptor at a time, so it should be safe in terms of file descriptor limits. To help `simeon` not have to do a second pass, you may increase the file descriptor limits of processes from your shell by running something like `ulimit -n 2000` before calling `simeon split` on Unix machines. For Windows users, you may have to dig into the Windows Registries for a corresponding setting. This should tell your OS kernel to allow OS processes to open up to 2000 file handles.


6. Care must be taken when using `simeon split` and `simeon push` to make sure that the number of positional arguments passed does not lead to the invoked command exceeding the maximum command-line length allowed for arguments in a command. To avoid errors along those lines, please consider passing the positional arguments as UNIX glob patterns. For instance, `simeon split --file-type log 'data/TRACKING-LOGS/*/*.log.gz'` tells `simeon` to expand the given glob pattern, instead of relying on the shell to do it.