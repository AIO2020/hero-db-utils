# Hero DB Utils

A python package to manage and read databases using pandas.

![Main branch tests passing](https://github.com/AIO2020/hero-db-utils/actions/workflows/run_tests.yml/badge.svg)
![Dev branch tests passing](https://github.com/AIO2020/hero-db-utils/actions/workflows/run_tests-dev.yml/badge.svg)

## Setup

Requires python >= 3.7

Install with pip by setting up your git SSH [deploy key](https://docs.github.com/en/developers/overview/managing-deploy-keys) as an environment variable:

```sh
export GIT_SSH_COMMAND='ssh -i ~/.ssh/<your-deploy-key>'
```

Then run:

```sh
pip install --upgrade git+ssh://git@github.com/AIO2020/hero-db-utils.git
```

If you prefer to use a [personal access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) you must set the variable `GIT_PERSONAL_ACCESS_TOKEN` to the value of the token and then run:

```sh
pip install --upgrade git+https://${GIT_PERSONAL_ACCESS_TOKEN}@github.com/AIO2020/hero-db-utils.git
```

## Use

Import the module as `hero_db_utils`

Example use:

```python
# Get params from environment variables:
from hero_db_utils.clients import PostgresDatabaseClient
conn_kwargs = PostgresDatabaseClient.get_params_from_env_variables()
client = PostgresDatabaseClient(**conn_kwargs, create_database=False)
# Example select all from table:
client.select("ed_dataset")
```

Check more examples at `notebooks/pg_client.ipynb`

## Unit Tests

There are tests available in the `tests` folder. Run them by setting the environment variables to connect to the database and then running:

```sh
python -m unittest
```

> You may set the environment variables in a `.env` file at the root of your project if you have `python-dotenv` installed.
