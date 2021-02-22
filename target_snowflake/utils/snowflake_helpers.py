from sqlalchemy import inspect


def schema_exists(snowflake_engine, schema):
    inspector = inspect(snowflake_engine)
    all_schema_names = inspector.get_schema_names()
    return schema.lower() in all_schema_names


def drop_snowflake_schema(snowflake_engine, db, schema):
    with snowflake_engine.connect() as connection:
        # Skip the CASCADE clause to be on the safe side while running tests
        connection.execute(f"DROP SCHEMA {db}.{schema}")


def drop_snowflake_table(snowflake_engine, db, schema, table):
    with snowflake_engine.connect() as connection:
        # Skip the CASCADE clause to be on the safe side while running tests
        connection.execute(f"DROP TABLE {db}.{schema}.{table}")


def get_reserved_keywords():
    """
    Return a set of all reservered words in Snowflake

    Used to quote attributes that are reserved words, like for example
    a Test table with an attribute Test.from or Test.to
    """
    return set(
        [
            "ALL",
            "ALTER",
            "AND",
            "ANY",
            "AS",
            "ASC",
            "BETWEEN",
            "BY",
            "CASE",
            "CAST",
            "CHECK",
            "CLUSTER",
            "COLUMN",
            "CONNECT",
            "CREATE",
            "CROSS",
            "CURRENT_DATE",
            "CURRENT_ROLE",
            "CURRENT_USER",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "DELETE",
            "DESC",
            "DISTINCT",
            "DROP",
            "ELSE",
            "EXCLUSIVE",
            "EXISTS",
            "FALSE",
            "FOR",
            "FOLLOWING",
            "FROM",
            "FULL",
            "GRANT",
            "GROUP",
            "HAVING",
            "IDENTIFIED",
            "ILIKE",
            "IMMEDIATE",
            "IN",
            "INCREMENT",
            "INNER",
            "INSERT",
            "INTERSECT",
            "INTO",
            "IS",
            "JOIN",
            "LATERAL",
            "LEFT",
            "LIKE",
            "LOCK",
            "LONG",
            "MAXEXTENTS",
            "MINUS",
            "MODIFY",
            "NATURAL",
            "NOT",
            "NULL",
            "OF",
            "ON",
            "OPTION",
            "OR",
            "ORDER",
            "REGEXP",
            "RENAME",
            "REVOKE",
            "RIGHT",
            "RLIKE",
            "ROW",
            "ROWS",
            "SAMPLE",
            "SELECT",
            "SET",
            "SOME",
            "START",
            "TABLE",
            "TABLESAMPLE",
            "THEN",
            "TO",
            "TRIGGER",
            "TRUE",
            "UNION",
            "UNIQUE",
            "UPDATE",
            "USING",
            "VALUES",
            "VIEW",
            "WHEN",
            "WHENEVER",
            "WHERE",
            "WITH",
        ]
    )
