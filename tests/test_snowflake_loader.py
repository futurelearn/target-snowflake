import json
import logging
import pytest

from datetime import datetime

from sqlalchemy import MetaData, Table, Column, inspect
from sqlalchemy.types import Float, String, BigInteger, Boolean
from snowflake.sqlalchemy import TIMESTAMP_NTZ

from target_snowflake.snowflake_loader import SnowflakeLoader
from target_snowflake.utils.snowflake_helpers import (
    schema_exists,
    drop_snowflake_schema,
)


@pytest.fixture(scope="class")
def test_table():
    """Acquire a predefined test sqlalchemy.Table"""
    demo_metadata = MetaData()

    return Table(
        "SNOWFLAKE_TARGET_TMP_TEST_TABLE",
        demo_metadata,
        Column("id", BigInteger, primary_key=True),
        Column("id2", String, primary_key=True),
        Column("str_attr", String),
        Column("float_attr", Float),
        Column("int_attr", BigInteger),
        Column("bool_attr", Boolean),
        Column("created_at", TIMESTAMP_NTZ),
    )


@pytest.fixture(scope="class")
def test_data():
    """Get a list of 8 records that follow the schema in test_table"""
    now = datetime.now()
    return [
        {
            "id": 1,
            "id2": "a1",
            "str_attr": "sadf",
            "float_attr": 1.111_111_111_1,
            "int_attr": 11,
            "bool_attr": True,
            "created_at": now,
        },
        {
            "id": 2,
            "id2": "b2",
            "str_attr": "qoiwensa",
            "float_attr": 2.222_222,
            "int_attr": 22,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 3,
            "id2": "c3",
            "str_attr": "jkwqheoi",
            "float_attr": 3.333_333,
            "int_attr": 33,
            "bool_attr": True,
            "created_at": now,
        },
        {
            "id": 4,
            "id2": "d4",
            "str_attr": "8q92qjkwlh",
            "float_attr": 4.4444,
            "int_attr": 44,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 5,
            "id2": "e5",
            "str_attr": "aoca.,209jk",
            "float_attr": 5.555,
            "int_attr": 55,
            "bool_attr": True,
            "created_at": now,
        },
        {
            "id": 6,
            "id2": "f6",
            "str_attr": "ma0s1-l,mf",
            "float_attr": 6.6666,
            "int_attr": 66,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 7,
            "id2": "g7",
            "str_attr": "na*&#@d",
            "float_attr": 7.777_777_7,
            "int_attr": 77,
            "bool_attr": True,
            "created_at": now,
        },
        {
            "id": 8,
            "id2": "h8",
            "str_attr": "m(*@&%l",
            "float_attr": 8.888_888_8,
            "int_attr": 88,
            "bool_attr": False,
            "created_at": now,
        },
    ]


@pytest.fixture(scope="class")
def test_data_upsert():
    """
    Get a list of 10 records that follow the schema in test_table.
    8 are updates over the 8 records from test_data and 2 are new records.
    """
    now = datetime.now()
    return [
        {
            "id": 1,
            "id2": "a1",
            "str_attr": "New_Value",
            "float_attr": 1.11,
            "int_attr": 11,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 2,
            "id2": "b2",
            "str_attr": "New_Value",
            "float_attr": 2.22,
            "int_attr": 22,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 3,
            "id2": "c3",
            "str_attr": "New_Value",
            "float_attr": 3.33,
            "int_attr": 33,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 4,
            "id2": "d4",
            "str_attr": "New_Value",
            "float_attr": 4.44,
            "int_attr": 44,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 5,
            "id2": "e5",
            "str_attr": "New_Value",
            "float_attr": 5.55,
            "int_attr": 55,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 6,
            "id2": "f6",
            "str_attr": "New_Value",
            "float_attr": 6.66,
            "int_attr": 66,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 7,
            "id2": "g7",
            "str_attr": "New_Value",
            "float_attr": 7.77,
            "int_attr": 77,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 8,
            "id2": "h8",
            "str_attr": "New_Value",
            "float_attr": 8.88,
            "int_attr": 88,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 9,
            "id2": "h9",
            "str_attr": "New Record",
            "float_attr": 9.99999,
            "int_attr": 99,
            "bool_attr": False,
            "created_at": now,
        },
        {
            "id": 10,
            "id2": "h10",
            "str_attr": "New Record",
            "float_attr": 10.10,
            "int_attr": 1010,
            "bool_attr": False,
            "created_at": now,
        },
    ]


class TestSnowflakeLoader:
    def test_connection(self, config, test_table):
        loader = SnowflakeLoader(table=test_table, config=config)

        with loader.engine.connect() as connection:
            assert connection

            results = connection.execute("select current_version()").fetchone()
            logging.info(f"Current Snowflake version: {results[0]}")

            assert results[0] is not None

    def test_schema_aply(self, config, test_table):
        loader = SnowflakeLoader(table=test_table, config=config)

        # Before running any integration test, check if the schema defined in
        #  the config is a new one (i.e. drop it afterwards)
        #  or an existing one (i.e. keep it - it could be the public one or
        #   a production schema used by mistake)
        new_schema = not schema_exists(loader.engine, test_table.schema)

        # Create the Test Table
        loader.schema_apply()

        # Check that both the schema and the table can be found in Snowflake
        inspector = inspect(loader.engine)

        all_schema_names = inspector.get_schema_names()
        assert test_table.schema.lower() in all_schema_names

        all_table_names = inspector.get_table_names(test_table.schema)
        assert test_table.name.lower() in all_table_names

        # Check that the Table created has the requested attributes
        expected_columns = [column.name for column in test_table.columns]

        columns = inspector.get_columns(test_table.name, schema=test_table.schema)
        for column in columns:
            assert column["name"].lower() in expected_columns

        # Call Again the schema_apply() function and make sure that nothing changed
        loader.schema_apply()

        columns = inspector.get_columns(test_table.name, schema=test_table.schema)

        assert len(columns) == 7

        # Wrap Up the test by destroying the Table created
        test_table.drop(loader.engine)

        # Drop the Schema if we created it and there is nothing left there
        inspector = inspect(loader.engine)
        all_table_names = inspector.get_table_names(config["schema"])
        if new_schema and (len(all_table_names) == 0):
            drop_snowflake_schema(loader.engine, config["database"], config["schema"])

    def test_load(self, config, test_table, test_data, test_data_upsert):
        loader = SnowflakeLoader(table=test_table, config=config)

        # check if the schema is a new one, ... etc ..
        new_schema = not schema_exists(loader.engine, test_table.schema)

        # Create the Test Table
        loader.schema_apply()

        # Load initial data (all inserts)
        loader.load(test_data)

        # Check that the correct number of rows were inserted
        query = f"SELECT COUNT(*) FROM {test_table.schema}.{test_table.name}"
        query2 = f"""
                   SELECT COUNT(*)
                   FROM {test_table.schema}.{test_table.name}
                   WHERE bool_attr = TRUE
                  """
        with loader.engine.connect() as connection:
            results = connection.execute(query).fetchone()
            assert results[0] == 8

            results = connection.execute(query2).fetchone()
            assert results[0] == 4

        # Test Upserting Data (8 updates && 2 inserts)
        loader.load(test_data_upsert)

        query3 = f"""
                   SELECT COUNT(*)
                   FROM {test_table.schema}.{test_table.name}
                   WHERE str_attr = 'New_Value'
                  """

        with loader.engine.connect() as connection:
            results = connection.execute(query).fetchone()
            assert results[0] == 10

            results = connection.execute(query2).fetchone()
            assert results[0] == 0

            results = connection.execute(query3).fetchone()
            assert results[0] == 8

        # Wrap Up the test by destroying the Table created
        test_table.drop(loader.engine)

        # Drop the Schema if we created it and there is nothing left there
        inspector = inspect(loader.engine)
        all_table_names = inspector.get_table_names(config["schema"])
        if new_schema and (len(all_table_names) == 0):
            drop_snowflake_schema(loader.engine, config["database"], config["schema"])
