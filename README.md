### simeon

`simeon` is a package and CLI tool to help with the processing of edx Research data


## Installations

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
git clone this_project
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


### List files
```sh
# List SQL files dumped since 2021-01-01
simeon list --site edx --org mitx --file-type sql --begin-date 2021-01-01
# List tracking log files dumped since 2021-01-01
simeon list --site edx --org mitx --file-type log --begin-date 2021-01-01
```


### Download, split and push SQL bundles
```sh
# Download a SQL bundle with the date 021-02-01 in its file name
simeon download --site edx --org mitx --file-type sql --begin-date 2021-02-01 --end-date 2021-02-01 --destination data/

# Download SQL bundles dumped any time since 2021-01-01 and extract the contents for course ID MITx/12.3x/1T2021
# Place the place in data/ and the output of the split in data/SQL
simeon download --site edx --org mitx --courses "MITx/12.3x/1T2021" --file-type sql --begin-date 2021-01-01 --destination data --split --split-destination data/SQL/

# Push to GCS the split up SQL files inside data/SQL/MITx__12_3x__1T2021
simeon push gcs --file-type sql --project ${GCP_PROJECT_ID} --bucket ${GCS_BUCKET} --service-account-file ${SAFILE} data/SQL/MITx__12_3x__1T2021

# Push the files to BigQuery and wait for the jobs to finish
# Using --use-storage tells BigQuery to extract the files to be loaded from Google Cloud Storage.
# So, use the option when you've already called simeon push gcs
simeon push gcs --wait-for-loads --use-storage --file-type sql --project ${GCP_PROJECT_ID} --bucket ${GCS_BUCKET} --service-account-file ${SAFILE} data/SQL/MITx__12_3x__1T2021
```


### Download, split and push Tracking logs
```sh
# Download a tracking log with the date 021-02-01 in its file name
simeon download --site edx --org mitx --file-type log --begin-date 2021-02-01 --end-date 2021-02-01 --destination data/

# Download tracking logs dumped any time since 2021-01-01 and extract the contents for course ID MITx/12.3x/1T2021
# Place the place in data/ and the output of the split in data/TRACKING_LOGS
simeon download --site edx --org mitx --courses "MITx/12.3x/1T2021" --file-type log --begin-date 2021-01-01 --destination data --split --split-destination data/TRACKING_LOGS/

# Push to GCS the split up tracking log files inside data/TRACKING_LOGS/MITx__12_3x__1T2021
simeon push gcs --file-type log --project ${GCP_PROJECT_ID} --bucket ${GCS_BUCKET} --service-account-file ${SAFILE} data/TRACKING_LOGS/MITx__12_3x__1T2021

# Push the files to BigQuery and wait for the jobs to finish
# Using --use-storage tells BigQuery to extract the files to be loaded from Google Cloud Storage.
# So, use the option when you've already called simeon push gcs
simeon push gcs --wait-for-loads --use-storage --file-type log --project ${GCP_PROJECT_ID} --bucket ${GCS_BUCKET} --service-account-file ${SAFILE} data/TRACKING_LOGS/MITx__12_3x__1T2021
```
