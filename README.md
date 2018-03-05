# nessie
Networked engines supply statistics in education.

## Installation

### Install Python 3

### Create your virtual environment

### Install dependencies

```
pip3 install -r requirements.txt [--upgrade]
```

### Create local configurations

If you plan to use any resources outside localhost, put your configurations in a separately encrypted area:

```
mkdir /Volumes/XYZ/nessie_config
export NESSIE_LOCAL_CONFIGS=/Volumes/XYZ/nessie_config
```

## Run tests, lint the code

We use [Tox](https://tox.readthedocs.io) for continuous integration. Under the hood, you'll find [PyTest](https://docs.pytest.org) and [Flake8](http://flake8.pycqa.org).
```
# Run all tests and linters
tox

# Pytest only
tox -e test

# Flake8 only
tox -e lint-py

# Run specific test(s)
tox -e test -- tests/test_models/test_authorized_user.py
tox -e test -- tests/test_externals/

# Lint specific file(s)
tox -e lint-py -- scripts/cohort_fixtures.py
```
