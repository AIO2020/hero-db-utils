from datetime import datetime
import numpy as np

import pytz
from hero_db_utils.queries.postgres import DBQuery, QueryFunc, QueryOp, QueryOperation
from hero_db_utils.testing import PostgresDatabaseBaseTest
from hero_db_utils.utils.dtypes import Literal

import pandas as pd


class PostgresDatabaseClientTests(PostgresDatabaseBaseTest):
    """
    Unittest class to test the PostgresDatabaseClient from the 
    database.client module.
    """

    FIXTURE_FP = "./tests/fixtures/end_prc.csv"
    TABLE_NAME = "patient_procedures"

    def setup_database(self):
        """
        Loads fixture into the database
        """
        # Read data from fixture:
        self.fixture_data = pd.read_csv(self.FIXTURE_FP)
        # Convert 'datetime' column to datetime:
        self.fixture_data["datetime"] = pd.to_datetime(
            self.fixture_data["datetime"],
            format=r"%d/%m/%Y %H:%M"
        )
        # Populate data into a table:
        self.pgclient.insert_from_df(
            self.fixture_data, self.TABLE_NAME,
        )

    def test_select_all(self):
        """
        Checks if gets all data in the table.
        """
        self.assertDataFrameEqual(
            self.fixture_data.sort_values(["datetime", "patient_id"]),
            self.pgclient.select(self.TABLE_NAME).sort_values(
                ["datetime", "patient_id"]
            ),
            ignore_idx=True,
            ignore_dtypes=True,
        )

    def test_select_with_params(self):
        """
        Checks that projection and other params work
        by comparing it with the edited fixture.
        """
        column_name = self.fixture_data.columns[0]
        # Get only one column from the df:
        one_col_df = self.fixture_data[[column_name]]
        query_result = self.pgclient.select(
            self.TABLE_NAME, projection=[column_name]
        )
        self.assertDataFrameEqual(
            one_col_df.sort_values(column_name),
            query_result.sort_values(column_name),
            ignore_idx=True,
        )

    def test_query(self):
        """
        Checks if returns the true value of a query.
        The query results itself do not matter, as long as the
        formats of the queries are equal the test is correct.
        """
        query = self.pgclient.build_query(
            "ed_procedures_dataset",
            projection=[
                "id",
                "start_date",
                "id_patient_emergency",
                "activity_id",
                "activity_name",
                "id_staff",
            ],
            filters={"activity_name": "Blood lab test", "id_staff": 15},
            order_by=["start_date"],
            ascending=False,
            limit=10,
        )
        self.assertEqual(
            self.pgclient.parse_query(query),
            (
                'SELECT "id","start_date","id_patient_emergency","activity_id","activity_name","id_staff" '
                'FROM "ed_procedures_dataset" WHERE ("activity_name" = \'Blood lab test\') AND ("id_staff" = 15) '
                'ORDER BY "start_date" DESC LIMIT 10'
            ),
        )

    def test_query_build_and_run(self):
        """
        Checks if the result of the client is the same as pandas.read_sql_query
        for the same query when is built from the params and then run.
        """
        # Unique patients with REGISTRATION procedures between 2020/01/01 and 2020/03/01
        from_date = "2020-01-01T00:00:00+00:00"
        to_date = "2020-01-03T00:00:00+00:00"
        query = (
            f'SELECT DISTINCT "patient_id","procedure_id" FROM "{self.TABLE_NAME}" '
            'WHERE ("procedure_label" = \'REGISTRATION\') AND ("datetime" BETWEEN '
            f"'{from_date}' AND '{to_date}') "
            'ORDER BY "patient_id" ASC LIMIT 50'
        )
        with self.pgclient.connection as conn:
            pd_results = pd.read_sql_query(query, con=conn,)
        query_obj = self.pgclient.build_query(
            self.TABLE_NAME,
            projection=["patient_id", "procedure_id"],
            filters={
                "procedure_label": "REGISTRATION",
                "datetime": QueryOp.between(from_date, to_date),
            },
            order_by=["patient_id"],
            limit=50,
            distinct=True,
        )
        # Check queries are equal:
        self.assertEqual(query, self.pgclient.parse_query(query_obj))
        # Check data returned is equal:
        pg_manager_results = self.pgclient.read_query(query_obj)
        self.assertDataFrameEqual(
            pg_manager_results.sort_values(["patient_id"]),
            pd_results.sort_values(["patient_id"]),
            ignore_idx=True,
        )

    def test_get_tables_from_schema(self):
        """
        Checks the `get_schema_tables` method of the postgres client.

        It should bring the same number of rows and columns
        as the only table there is in the fixture data.
        """
        schema_tables = self.pgclient.get_schema_tables()
        self.assertEqual(
            len(schema_tables.index), 1,
            msg="There should only be one row (table) in the schema tables got from the client."
        )
        schema_table_ser = schema_tables.set_index("table_name").loc[self.TABLE_NAME]
        nrows_cl = schema_table_ser["nrows"]
        ncols_cl = schema_table_ser["ncols"]
        self.assertEqual(
            nrows_cl, len(self.fixture_data.index),
            msg="Number of rows in schema_tables info is not the same as the fixture data"
        )
        self.assertEqual(
            ncols_cl, len(self.fixture_data.columns),
            msg="Number of columns in schema_tables info is not the same as the fixture data"
        )

    def test_count_rows(self):
        """
        Checks the count rows method of the postgres client.
        """
        count = self.pgclient.count(self.TABLE_NAME)
        self.assertEqual(count, len(self.fixture_data.index))

    def test_build_query_object(self):
        """
        Checks if a query can be built manually using an OOP approach.
        """
        from_date = datetime(
            year=2020, month=1, day=1, hour=0, minute=0, tzinfo=pytz.utc
        )
        to_date = datetime(
            year=2020, month=1, day=1, hour=1, minute=30, tzinfo=pytz.utc
        )
        # Select outcomes between two dates:
        sql_query = (
            f'SELECT * FROM "{self.TABLE_NAME}" '
            "WHERE (((\"procedure_label\" = 'Registration') AND (\"place\" = 'Waiting Room')) "
            "OR (\"place\" = 'Hospitalization') OR (\"place\" = 'Death') "
            "OR (\"place\" = 'Discharge') OR (\"place\" = 'Discharge and LWBS')) "
            "AND (\"datetime\" BETWEEN '{from_date}' AND '{to_date}') "
            'ORDER BY "datetime","patient_id" ASC LIMIT 1000'
        ).format(from_date=from_date.isoformat(), to_date=to_date.isoformat())
        # Build query using DBQuery object:
        query_obj = DBQuery()
        place_statement = QueryOperation.q_or(
            QueryOperation.q_and(
                mapping={
                    "procedure_label":"Registration",
                    "place":"Waiting Room"
                }
            ),
            values=[
                "Hospitalization",
                "Death",
                "Discharge",
                "Discharge and LWBS",
            ],
            key="place"
        )
        place_statement.join_and(
            QueryOp.between(from_date, to_date).resolve("datetime")
        )
        query_obj.where(place_statement)
        query_obj.order_by("datetime", "patient_id").limit(1000).table(self.TABLE_NAME)
        # Resolve query:
        query_obj.resolve()
        client_sql_query = self.pgclient.parse_query(query_obj)
        # Check sql queries are equal:
        self.assertEqual(sql_query, client_sql_query)
        # Run queries:
        obj_result = self.pgclient.read_query(query_obj)
        sql_result = self.pgclient.read_sql_query(sql_query)
        # Check results are equal:
        self.assertDataFrameEqual(sql_result, obj_result)

    def test_full_query(self):
        """
        Tests the result of a query built with all supported sql clauses.
        """
        # Create patients table to use for join:
        patients = (
            self.fixture_data[["patient_id"]]
            .dropna()
            .drop_duplicates()
            .sort_values("patient_id")
            .reset_index(drop=True)
            .reset_index()
        )
        patients = patients.rename(columns={"index": "id"})
        patients["id"] = patients["id"] + 1
        patients["age"] = np.random.randint(5, 90, size=len(patients.index))
        self.pgclient.insert_from_df(patients, "patients", drop=True)
        # Create query:
        from_date = datetime(
            year=2020, month=1, day=1, hour=0, minute=0
        )
        to_date = datetime(
            year=2020, month=1, day=1, hour=1, minute=30
        )
        from_date2 = datetime(
            year=2020, month=1, day=2, hour=7, minute=0
        )
        to_date2 = datetime(
            year=2020, month=1, day=2, hour=23, minute=59
        )
        sql_query = (
            'SELECT DISTINCT "{table_name}"."patient_id" AS "patid","procedure_id","age",COUNT("place") AS "places_count" FROM "{table_name}" '
            'LEFT JOIN "patients" ON ("{table_name}"."patient_id" = "patients"."patient_id") WHERE '
            "(\"datetime\" BETWEEN '{from_date}' AND '{to_date}') OR "
            "(\"datetime\" BETWEEN '{from_date2}' AND '{to_date2}') "
            'GROUP BY "{table_name}"."patient_id","procedure_id","age" '
            'HAVING (COUNT("place") > 1) ORDER BY COUNT("place") DESC LIMIT 20'
        ).format(
            from_date=from_date.isoformat(),
            to_date=to_date.isoformat(),
            from_date2=from_date2.isoformat(),
            to_date2=to_date2.isoformat(),
            table_name=self.TABLE_NAME,
        )
        # Build query filter:
        filter_q = QueryOperation.q_or(
            QueryOp.between(from_date, to_date).resolve("datetime"),
            QueryOp.between(from_date2, to_date2).resolve("datetime"),
        )
        # Build query using all params:
        client_query = self.pgclient.build_query(
            self.TABLE_NAME,
            projection=[
                QueryFunc.relation(self.TABLE_NAME, "patient_id", alias="patid"),
                "procedure_id",
                "age",
                QueryFunc.count("place", alias="places_count"),
            ],
            limit=20,
            filters=filter_q,
            group_by=[
                QueryFunc.relation(self.TABLE_NAME, "patient_id"),
                "procedure_id",
                "age",
            ],
            having_filters={QueryFunc.count("place"):QueryOp.greater_than(1)},
            order_by=[QueryFunc.count("place")],
            join_table="patients",
            join_on={
                QueryFunc.relation(self.TABLE_NAME, "patient_id"): QueryFunc.relation(
                    "patients", "patient_id"
                )
            },
            join_how="left",
            ascending=False,
            distinct=True,
        )
        # Check if queries match:
        client_sql_query = self.pgclient.parse_query(client_query)
        self.assertEqual(sql_query, client_sql_query)
        client_result = self.pgclient.read_query(client_query)
        sql_result = self.pgclient.read_sql_query(sql_query)
        # Check if dataframes match:
        self.assertDataFrameEqual(
            client_result, sql_result, ignore_idx=True, ignore_dtypes=True
        )
    
    def test_literal_object_select(self):
        """
        Checks that you can add a 'literal' sql value in the projection
        of a the postgres client's 'build_query' method using a Literal object.
        """
        sql_query = (
            "SELECT DISTINCT \"patient_id\" AS \"patid\","
            "CONCAT(procedure_id, ', ', procedure_label, ' done') "
            "AS procedure_id_label FROM \"{}\""
        ).format(self.TABLE_NAME)
        client_query = self.pgclient.build_query(
            self.TABLE_NAME,
            projection=[
                QueryFunc.alias("patient_id", alias="patid"),
                Literal(
                    "CONCAT(procedure_id, ', ', procedure_label, ' done') "
                    "AS procedure_id_label"
                )
            ],
            distinct=True
        )
        client_sql_query = self.pgclient.parse_query(client_query)
        self.assertEqual(sql_query, client_sql_query)
        client_result = self.pgclient.read_query(client_query)
        sql_result = self.pgclient.read_sql_query(sql_query)
        self.assertDataFrameEqual(
            sql_result, client_result,
        )

    def test_literal_object_value(self):
        """
        Checks that you can use a Literal object as a query value
        in the postgres client's 'build_query' method.
        """
        sql_query = (
            "SELECT \"procedure_id\",COUNT(*) AS procedure_count "
            "FROM \"{}\" GROUP BY \"procedure_id\" "
            "HAVING (COUNT(*) > 20-10) ORDER BY \"procedure_count\" ASC"
        ).format(self.TABLE_NAME)
        client_query = self.pgclient.build_query(
            self.TABLE_NAME,
            projection=[
                "procedure_id",
                QueryFunc.count(alias="procedure_count"),
            ],
            having_filters={Literal("COUNT(*)"):QueryOp.greater_than(Literal("20-10"))},
            group_by=["procedure_id"],
            order_by=["procedure_count"]
        )
        client_sql_query = self.pgclient.parse_query(client_query)
        self.assertEqual(sql_query, client_sql_query)
        client_result = self.pgclient.read_query(client_query)
        sql_result = self.pgclient.read_sql_query(sql_query)
        self.assertDataFrameEqual(
            sql_result, client_result,
        )

    def test_raw_sql_query(self):
        """
        Checks the 'read_sql_query' method from the postgres client
        by making sure it returns the same dataframe as the
        'read_sql_query' from pandas.
        """
        sql_query = """
        SELECT "place", count("place") AS place_count FROM {} GROUP BY "place"
        HAVING count("place") > 100 ORDER BY place_count DESC LIMIT 10
        """.format(
            self.TABLE_NAME
        )
        with self.pgclient.connection as conn:
            pd_result = pd.read_sql_query(sql_query, con=conn)
        cl_result = self.pgclient.read_sql_query(sql_query)
        self.assertDataFrameEqual(
            pd_result, cl_result,
        )

    def test_select_offset(self):
        """
        Checks the 'offset' param in the build query method.
        """
        sql_query = (
            "SELECT * FROM \"{}\" ORDER BY \"place\" ASC OFFSET 10 ROWS LIMIT 5"
        ).format(
            self.TABLE_NAME
        )
        sql_result = self.pgclient.read_sql_query(sql_query)
        cl_query = self.pgclient.build_query(
            self.TABLE_NAME,
            order_by=["place"],
            offset=10,
            limit=5
        )
        client_sql_query = self.pgclient.parse_query(cl_query)
        self.assertEqual(sql_query, client_sql_query)
        cl_result = self.pgclient.read_query(cl_query)
        self.assertDataFrameEqual(
            sql_result, cl_result,
        )

if __name__ == "__main__":
    from unittest import main

    main()
