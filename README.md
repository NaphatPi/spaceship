# Spaceship

A Python library to create a dataset in [Delta Lake](https://delta.io/) format, add new data to the dataset with automatic schema validation.
It also comes with abilities to query the data directly using SQL. Spaceship supports both local and cloud object storage like S3 and Digital Ocean Spaces.

## Installation

```bash
pip install git+https://github.com/NaphatPi/spaceship.git
```

## Quickstart
See [quickstart jupyter notebook](/demo/quickstart.ipynb).

## For development

```bash
# clone the repo
git clone https://github.com/naphatpi/spaceship

# install the dev dependencies
make install

# run linting
make lint

# run the tests
make test
```
