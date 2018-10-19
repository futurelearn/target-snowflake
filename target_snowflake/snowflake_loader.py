import os
import logging

from typing import Dict, Iterable
from sqlalchemy import create_engine, inspect, Table
from sqlalchemy.schema import CreateSchema
from snowflake.sqlalchemy import URL


# Don't show all the info log messages from Snowflake
for logger_name in ["snowflake.connector", "botocore", "boto3"]:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)


class SnowflakeLoader:
    def __init__(self, table: Table, config: Dict) -> None:
        self.table = table

        # Add a schema to the provided sqlalchemy Table as it is agnostic
        #  on wich schema we want to use (defined in config)
        self.table.schema = config["schema"]

        self.database = config["database"]
        self.role = config["role"]

        self.engine = create_engine(
            URL(
                user=config["username"],
                password=config["password"],
                account=config["account"],
                database=config["database"],
                role=config["role"],
                warehouse=config["warehouse"],
            )
        )

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

    def schema_apply(self) -> None:
        """
        Apply the schema defined for self.table to the Database we connect to

        At the moment no schema updates are supported.
        """
        schema_updated = False
        inspector = inspect(self.engine)

        all_schema_names = inspector.get_schema_names()
        if not (self.table.schema.lower() in all_schema_names):
            logging.debug(f"Schema {self.table.schema} does not exist -> creating it ")
            self.engine.execute(CreateSchema(self.table.schema))
            schema_updated = True

        all_table_names = inspector.get_table_names(self.table.schema)
        if not (self.table.name.lower() in all_table_names):
            logging.debug(f"Table {self.table.name} does not exist -> creating it ")
            self.table.create(self.engine)
            schema_updated = True

        if schema_updated:
            self.grant_privileges(self.role)

    def load(self, data: Iterable[Dict]) -> None:
        """
        Load the data provided as a list of dictionaries to the given Table

        If there are Primary Keys defined, then we UPSERT them by loading
        the data to a temporary table and then using Snowflake's MERGE operation
        """
        if len(data) > 0:
            logging.debug(f"Loading data to Snowflake for {self.table.name}")

            if len(self.table.primary_key) > 0:
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
        target_table = f"{self.table.schema}.{self.table.name}"
        source_table = f"{self.table.schema}.{tmp_table_name}"

        # Join using all primary keys
        joins = []
        for primary_key in self.table.primary_key:
            pk = primary_key.name
            joins.append(f"{target_table}.{pk} = {source_table}.{pk}")
        join_expr = " AND ".join(joins)

        attributes_target = []
        attributes_source = []
        update_sub_clauses = []

        for column in self.table.columns:
            attr = column.name
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
