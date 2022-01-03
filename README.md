# Hero DB Utils

A python package to manage and read databases using pandas.

![Main branch tests passing](https://github.com/AIO2020/hero-db-utils/actions/workflows/run_tests.yml/badge.svg)
![Dev branch tests passing](https://github.com/AIO2020/hero-db-utils/actions/workflows/run_tests-dev.yml/badge.svg)

## Setup

Requires python >= 3.7

Install with pip by running the command:

```sh
pip install --upgrade git+https://git@github.com/AIO2020/hero-db-utils.git
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
