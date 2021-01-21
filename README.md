### simeon

`simeon` is a package and CLI tool to help with the processing of edx Research data

### Developing
```sh
git clone this_project
cd simeon
# Set up a virtual environment if you don't already have on
python3 -m venv venv
. venv/bin/activate
# Install the packages needed for developing
pip install -r requirements_dev.txt
# pip install the package in an editable way
pip install -e .
# Invoke the executable
simeon --help
# Run the tests
python setup.py test
# Write code and tests and submit PR's
```

### TODO:
- Write a mock class for the `S3Blob` class so we can use it in unit tests
- Add logic to split incremental logs into separate log files while reformatting them
    1. Write a function to process a JSON line: [Something like this](https://github.com/mitodl/edx2bigquery/blob/8a1efefaa36fa5cd455f5bbd886c3d3f70be33e6/edx2bigquery/split_and_rephrase.py#L63), but cleaner.
    2. Write a function that uses the results of number `1.` to write lines to their corresponding files
- Add logic to compute special table like `person_course_day` and `person_course`. The `make_*` modules from the [`edx2bigquery` package](https://github.com/mitodl/edx2bigquery/tree/master/edx2bigquery) should help with this process.
- Update the CLI script to handle commands like `download`, `split`, `compute`, `push`, etc.