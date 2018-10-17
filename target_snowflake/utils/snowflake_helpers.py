from sqlalchemy import inspect


def schema_exists(snowflake_engine, schema):
    inspector = inspect(snowflake_engine)
    all_schema_names = inspector.get_schema_names()
    return schema.lower() in all_schema_names


def drop_snowflake_schema(snowflake_engine, db, schema):
    with snowflake_engine.connect() as connection:
        # Skip the CASCADE clause to be on the safe side while running tests
        connection.execute(f"DROP SCHEMA {db}.{schema}")
