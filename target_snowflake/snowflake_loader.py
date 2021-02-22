import os
import logging
import functools

from typing import Dict, List
from sqlalchemy import create_engine, inspect, Table
from sqlalchemy.schema import CreateSchema
from snowflake.sqlalchemy import URL, TIMESTAMP_NTZ
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.network import ReauthenticationRequest

from target_snowflake.utils.error import SchemaUpdateError
from target_snowflake.utils.snowflake_helpers import get_reserved_keywords


# Don't show all the info log messages from Snowflake
for logger_name in ["snowflake.connector", "botocore", "boto3"]:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)


# Map sqlalchemy types to Snowflake Types
# Required for two reasons:
# 1. Compare the sqlalchemy Table definition to what is defined in Snowflake
# 2. Use the type to manually execute an ALTER TABLE for updating or
#    adding new columns
MAP_SQLALCHEMY_TO_SNOWFLAKE_TYPE = {
    "BIGINT": "DECIMAL(38, 0)",
    "FLOAT": "FLOAT",
    "VARCHAR": "VARCHAR(16777216)",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP_NTZ",
}


# Type updates allowed.
# There is a limitation in Snowflake that really limits the possible transitions:
#  https://docs.snowflake.net/manuals/sql-reference/sql/alter-table-column.html
#  When setting the TYPE for a column, the specified type (i.e. type)
#   must be a text data type (VARCHAR, STRING, TEXT, etc.).
#  Also, TYPE can be used only to increase the length of a text column.
# That means that no INT --> FLOAT or INT --> STRING, etc type upgrades
#  are allowed by Snowflake.
# Together with the fact that sqlalchemy uses the maximum type length,
#  only the following 2 type upgrades are valid (which never occur
#  but are added for completeness and in order to support updates in
#  SnowflakeLoader for future proofing):
ALLOWED_TYPE_TRANSITIONS = [
    ("VARCHAR(16777216)", "STRING"),
    ("VARCHAR(16777216)", "TEXT"),
]


# How many times are we going to try to run functions with
# @handle_token_expiration when they raise exceptions.
TokenExpirationMaxTries = 2


def handle_token_expiration(func):
    """
    Wrap SnowflakeLoader methods in order to catch token expiration errors,
    refresh the engine and retry.

    If the session stays idle for 4 hours, then the master token that
    snowflake.sqlalchemy has stored expires and a new session token can not be
    automatically renewed.

    In that case, the following exceptions are raised:
    snowflake.connector.errors.ProgrammingError: 390114 (08001)
    snowflake.connector.network.ReauthenticationRequest: 390114 (08001)
    Authentication token has expired. The user must authenticate again.

    We only retry once:
    The first try is the normal excecution that will fail if 4 hours have passed
    since the last query.
    The second try follows a refresh_engine() and should succeed.
    If it fails again, then something else happens and we should stop the execution
    and report the error.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        last_exception = None

        for retry in range(TokenExpirationMaxTries):
            try:
                return func(self, *args, **kwargs)
            except (ProgrammingError, ReauthenticationRequest) as exc:
                if "390114" in str(exc):
                    last_exception = exc
                    self.refresh_engine()
                else:
                    raise exc

        # If we tried TokenExpirationMaxTries times and we keep on getting errors,
        #  just stop trying and raise the last exception caught
        raise last_exception

    return wrapper


class SnowflakeEngineFactory:
    def __init__(self, config: Dict) -> None:
        # Keep the config in the EngineFactory in order to be able to refresh
        #  the engine if the master token expires
        self._config = config

    def create_engine(self):
        return create_engine(
            URL(
                user=self._config["username"],
                password=self._config["password"],
                account=self._config["account"],
                database=self._config["database"],
                role=self._config["role"],
                warehouse=self._config["warehouse"],
            )
        )


class SnowflakeLoader:
    def __init__(self, table: Table, config: Dict) -> None:
        self.table = table

        # Add a schema to the provided sqlalchemy Table as it is agnostic
        #  on wich schema we want to use (defined in config)
        self.table.schema = config["schema"]

        # Keep the database name and the role name as they are required
        #  for granting privileges to new entities.
        self.database = config["database"]
        self.role = config["role"]

        # Create a SnowflakeEngineFactory with the provided config
        #  and use it to generate a new engine for connecting to Snowflake
        self._engine_factory = SnowflakeEngineFactory(config)
        self.engine = self._engine_factory.create_engine()

    def refresh_engine(self) -> None:
        if self.engine:
            self.engine.dispose()

        self.engine = self._engine_factory.create_engine()

    def quoted_table_name(self) -> str:
        """
        Get the FULL, quoted, table name with everything in caps.
        e.g. "TEST_DB"."TARGET_SNOWFLAKE_TEST"."TEST_TABLE"
        """
        return f'"{self.database}"."{self.table.schema}"."{self.table.name.upper()}"'

    def attribute_names(self) -> List[str]:
        """
        Get the attribute(column) names for the associated Table
        """
        return [column.name for column in self.table.columns]

    def empty_record(self) -> Dict:
        """
        Get a dictionary representing an empty (all attributes None) record for
        the table associated with this SnowflakeLoader instance.

        Used as a template in order to normalize (map) all imported records to
        the full schema they are defined for.

        Important for records with multiple optional attributes that are not
        always there, like for example Multi Level JSON objects that are
        flattened before uploaded to SNowflake.

        Guards against sqlalchemy errors for missing required values for
        bind parameters.
        """
        return dict.fromkeys(column.name for column in self.table.columns)

    @handle_token_expiration
    def schema_apply(self) -> None:
        """
        Apply the schema defined for self.table to the Database we connect to
        """
        grant_required = False
        inspector = inspect(self.engine)

        all_schema_names = inspector.get_schema_names()
        if not (self.table.schema.lower() in all_schema_names):
            logging.debug(f"Schema {self.table.schema} does not exist -> creating it ")
            self.engine.execute(CreateSchema(self.table.schema))
            grant_required = True

        all_table_names = inspector.get_table_names(self.table.schema)
        if not (self.table.name.lower() in all_table_names):
            logging.debug(f"Table {self.table.name} does not exist -> creating it ")
            self.table.create(self.engine)
            grant_required = True
        else:
            # There is an existing Table: Check if a schema update is required
            self.schema_update(inspector)

        if grant_required:
            self.grant_privileges(self.role)

    def schema_update(self, inspector) -> None:
        """
        Check if there is a schema diff between the new Table and the existing
        one and if the changes can be supported, update the table with the diff.

        Rules:
        1. Only support type upgrades (e.g. STRING -> VARCHAR) for existing columns
        2. If a not supported type update is requested (e.g. float --> int)
           raise a SchemaUpdateError exception.
        2. Never drop columns, only update or add new ones
        """
        existing_columns = {}
        columns_to_add = []
        columns_to_update = []

        # Fetch the existing defined tables and store them in a format useful
        #  for comparisors.
        all_columns = inspector.get_columns(self.table.name, schema=self.table.schema)

        for column in all_columns:
            if isinstance(column["type"], TIMESTAMP_NTZ):
                existing_columns[column["name"]] = "TIMESTAMP_NTZ"
            else:
                existing_columns[column["name"]] = f"{column['type']}"

        # Check the new Table definition for new attributes or attributes
        #  with an updated data type
        for column in self.table.columns:
            if isinstance(column.type, TIMESTAMP_NTZ):
                column_type = "TIMESTAMP_NTZ"
            else:
                column_type = MAP_SQLALCHEMY_TO_SNOWFLAKE_TYPE[f"{column.type}"]

            if column.name not in existing_columns:
                # A new column to be added to the table
                columns_to_add.append((column.name, column_type))
            elif column_type != existing_columns[column.name]:
                # An existing column with a different data type
                # Check if the update is allowed
                transition = (existing_columns[column.name], column_type)
                if transition not in ALLOWED_TYPE_TRANSITIONS:
                    raise SchemaUpdateError(
                        f"Not allowed type update for {self.table.name}.{column.name}: {transition}"
                    )
                columns_to_update.append((column.name, column_type))

        # If there are any columns to add or update, make the schema update
        for (name, type) in columns_to_add:
            self.add_column(name, type)

        for (name, type) in columns_to_update:
            self.update_column(name, type)

    def add_column(self, col_name: str, col_data_type: str) -> None:
        """
        Add the requested column to the Snowflake Table defined by self.table
        """
        full_name = self.quoted_table_name()
        alter_stmt = f"ALTER TABLE {full_name} ADD COLUMN {col_name} {col_data_type}"

        logging.debug(f"Adding COLUMN {col_name} ({col_data_type}) to {full_name}")

        with self.engine.connect() as connection:
            connection.execute(alter_stmt)

    def update_column(self, col_name: str, col_data_type: str) -> None:
        """
        Update the requested column to the new type col_data_type
        """
        full_name = self.quoted_table_name()
        alter_stmt = f"ALTER TABLE {full_name} ALTER {col_name} TYPE {col_data_type}"

        logging.debug(f"ALTERING COLUMN {full_name}.{col_name} to {col_data_type}")

        with self.engine.connect() as connection:
            connection.execute(alter_stmt)

    @handle_token_expiration
    def load(self, data: List[Dict]) -> None:
        """
        Load the data provided as a list of dictionaries to the given Table

        If there are Primary Keys defined, then we UPSERT them by loading
        the data to a temporary table and then using Snowflake's MERGE operation
        """
        if not data:
            return

        logging.debug(f"Loading data to Snowflake for {self.table.name}")
        if self.table.primary_key:
            # We have to use Snowflake's Merge in order to Upsert

            # Create Temporary table to load the data to
            tmp_table = self.create_tmp_table()

            with self.engine.connect() as connection:
                connection.execute(tmp_table.insert(), data)

                # Merge Temporary Table into the Table we want to load data into
                merge_stmt = self.generate_merge_stmt(tmp_table.name)
                connection.execute(merge_stmt)

            # Drop the Temporary Table
            tmp_table.drop(self.engine)
        else:
            # Just Insert (append) as no conflicts can arise
            with self.engine.connect() as connection:
                connection.execute(self.table.insert(), data)

    def create_tmp_table(self) -> Table:
        """
        Create a temporary table in Snowflake based on the Table definition we
        have stored in self.table.
        """
        columns = [c.copy() for c in self.table.columns]
        tmp_table = Table(
            f"TMP_{self.table.name.upper()}",
            self.table.metadata,
            *columns,
            schema=self.table.schema,
            keep_existing=True,
        )

        tmp_table.drop(self.engine, checkfirst=True)
        tmp_table.create(self.engine)

        return tmp_table

    def generate_merge_stmt(self, tmp_table_name: str) -> str:
        """
        Generate a merge statement for Merging a temporary table into the
          main table.

        The Structure of Merge in Snowflake is as follows:
          MERGE INTO <target_table> USING <source_table>
            ON <join_expression>
          WHEN MATCHED THEN UPDATE SET <update_clause>
          WHEN NOT MATCHED THEN INSERT (source_atts) VALUES {insert_clause}

        In this simple form, it has the same logic as UPSERT in Postgres
            INSERT ... ... ...
            ON CONFLICT ({pkey}) DO UPDATE SET {update_clause}
        """
        reserved_keywords = get_reserved_keywords()

        target_table = f"{self.table.schema}.{self.table.name}"
        source_table = f"{self.table.schema}.{tmp_table_name}"

        # Join using all primary keys
        joins = []
        for primary_key in self.table.primary_key:
            pk = str(primary_key.name)

            if pk.upper() in reserved_keywords:
                pk = f'"{pk}"'

            joins.append(f"{target_table}.{pk} = {source_table}.{pk}")
        join_expr = " AND ".join(joins)

        attributes_target = []
        attributes_source = []
        update_sub_clauses = []

        for column in self.table.columns:
            attr = str(column.name)
            if attr.upper() in reserved_keywords:
                attr = f'"{attr}"'

            attributes_target.append(attr)
            attributes_source.append(f"{source_table}.{attr}")

            if attr not in self.table.primary_key:
                update_sub_clauses.append(f"{attr} = {source_table}.{attr}")

        update_clause = ", ".join(update_sub_clauses)
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_clause}"

        source_attributes = ", ".join(attributes_target)
        target_attributes = ", ".join(attributes_source)

        insert_clause = f"({source_attributes}) VALUES ({target_attributes})"
        not_matched_clause = f"WHEN NOT MATCHED THEN INSERT {insert_clause}"

        merge_stmt = f"MERGE INTO {target_table} USING {source_table} ON {join_expr} {matched_clause} {not_matched_clause}"

        return merge_stmt

    def grant_privileges(self, role: str) -> None:
        """
        GRANT access to the created schema and tables to the provided role
        """
        with self.engine.connect() as connection:
            db_name = self.database
            grant_stmt = f'GRANT USAGE ON SCHEMA "{db_name}"."{self.table.schema}" TO ROLE {role};'
            connection.execute(grant_stmt)
            grant_stmt = f'GRANT SELECT ON ALL TABLES IN SCHEMA "{db_name}"."{self.table.schema}" TO ROLE {role};'
            connection.execute(grant_stmt)
