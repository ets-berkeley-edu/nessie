# Nessie

![Picture of Loch Ness, Scotland](public/loch-ness.jpg)

Networked engines supply statistics in education.

## Installation

* Install Python 3.8
* Create your virtual environment (venv)
* Install dependencies
```
pip3 install virtualenv
virtualenv venv
source venv/bin/activate

pip3 install -r requirements.txt [--upgrade]
```

### Front-end dependencies

[Vue CLI 3](https://cli.vuejs.org/) was used to provision the front-end.
```
nvm use
npm install
```

### Postgres user and databases

```
createuser nessie --no-createdb --no-superuser --no-createrole --pwprompt
createdb nessie --owner=nessie
createdb nessie_metadata_test --owner=nessie
createdb nessie_redshift_test --owner=nessie
createdb nessie_test --owner=nessie
```

### Create local configurations

If you plan to use any resources outside localhost, put your configurations in a separately encrypted area:
```
mkdir /Volumes/XYZ/nessie_config
export NESSIE_LOCAL_CONFIGS=/Volumes/XYZ/nessie_config
```

## Fire it up!

1. Open up a terminal and start the Python app:
```
python3 application.py
```
2. Nessie back-end APIs now available at http://localhost:5001
3. If you are doing front-end development then open a second terminal and
use the underlying `vue-cli-service` to start Vue.js app. Compile and hot-reloads for dev:
```
npm run serve-vue
```
4. Nessie Admin Console (front-end) is now available at http://localhost:8080

## Run tests, lint the code

We use [Tox](https://tox.readthedocs.io) for continuous integration. Under the hood, you'll find [PyTest](https://docs.pytest.org) and [Flake8](http://flake8.pycqa.org).
```
# Run all tests and linters
tox

# Pytest
tox -e test

# Run specific test(s)
tox -e test -- tests/test_models/test_authorized_user.py
tox -e test -- tests/test_externals/

# Lint
tox -e lint-py
tox -e lint-vue

# Lint specific file(s)
tox -e lint-py -- scripts/cohort_fixtures.py

# Run testext tests

Tests marked `@pytest.mark.testext` require actual connections to external services, and are not run as part of a normal tox execution. They can be run by directly invoking PyTest with the 'testext' environment specified.

NESSIE_ENV=testext pytest

Configuration for testext runs can be placed in a testext-local.py file under your NESSIE_LOCAL_CONFIGS directory. See config/testext.py for a model.
```
