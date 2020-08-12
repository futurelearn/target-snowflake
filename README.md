# target-snowflake

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
and loads them to Snowflake.


## Installation

1. Create and activate a virtualenv
2. `pip install -e '.[dev]'`  
3. Create an .env file to hold the environment variables listed below (SF_ACCOUNT,SF_USER,etc...)
4. Execute `python target_snowflake/utils/config_generator.py` to generate a proper config.json as described below


## Configuration of Snowflake Target

**config.json**
```json
{
  "account": "Account Name in Snowflake (https://XXXXX.snowflakecomputing.com), https://docs.snowflake.net/manuals/user-guide/connecting.html#your-snowflake-account-name-and-url",
  "username": "",
  "password": "",
  "role": "Role to be used for loading the data, e.g. LOADER. Also this role is GRANTed usage to all tables and schemas created",
  "database": "The DB to be used",
  "schema": "Schema to be used",
  "warehouse": "Snowflake Warehouse used for loading the data",

  "batch_size": "How many records are sent to Snowflake at a time? Default=5000",

  "timestamp_column": "Name of the column used for recording the timestamp when Data are uploaded to Snowflake. Default=__loaded_at"
}
```

## Using the Makefile

Any command you should need is contained within the Makefile. When needed, the Makefile will generate a config for you based on the environment variables available in your shell.
The required env vars are as follows:

```json
{
  "account": SF_ACCOUNT,
  "username": SF_USER,
  "password": SF_PASSWORD,
  "role": SF_ROLE,
  "database": SF_DATABASE,
  "schema": SF_TEST_SCHEMA,
  "warehouse": SF_WAREHOUSE
}
```  

The following commands are not test-related but still useful:

1. `make build`: this builds an image of target-snowflake with code that reflects the current state of the repo
2. `make mypy`: runs the mypy type-checker on the target_snowflake folder
3. `make lint`: runs a strict python linter on the `target_snowflake` and `tests` folder. Changes the code in-place


## Simple test run

If you want to quickly test that your setup is properly configured, you can create a couple of test Tables and send some test data to a Snowflake Database for your Account.

`make test-run`


## Running Tests

Testing the Snowflake Target requires connecting with Snowflake, this is automatically created within the `make` command.

In the following examples, we assume that an `SF_TEST_SCHEMA` environment variable exists and points to a safe, non existent test schema. For example, we use `"schema": "TARGET_SNOWFLAKE_TEST"` so that it can be created, populated with test tables and data and destroyed when the tests are completed.

If the requested schema is not found in the Database, it is both created and destroyed when the tests finish. In contrast, if an already existing schema is provided, the tests run, create the test table(s) and destroy them in the end but do not drop the schema. This is a deliberate choice in order to protect users from running with sufficient rights the tests using a config that points to public, INFORMATION_SCHEMA or a production schema and then accidentally destroying it and its tables.

### Testing the Snowfake Loader

This includes a set of simple tests to check that the connection to Snowflake is properly set and that all the required Snowflake operations work as expected.

`make test-quick`

During the tests we create a test table(SNOWFLAKE_TARGET_TMP_TEST_TABLE), populate it with simple data, assert that both the schema and the data loaded are as expected and in the end we destroy it.

### Integration Tests for target-snowflake

We have also full integration tests for testing the Snowflake Target pipeline end-to-end by using precrafted test streams that are located in `tests/data_files/`.

`make test-full`

Those are extensive tests that run the full pipeline exactly in the same way as target-snowflake would run for the same configuration and inputs and then check the created tables and uploaded data that everything went according to plan.

Those tests may take a little bit more than simple unit tests depending on the connection to Snowflake, so we do not advise running them constantly (e.g. in a CI/CD pipeline).

## Implementation Notes

There are some implicit decisions on the implementation of this Target:

*  Data are UPSERTed when an entity has at least one primary key (key_properties not empty). If there is already a row with the same
composite key (combination of key_properties) then the new record UPDATEs the existing one.

    In order for this TARGET to work on append only mode and the target tables to store historical information, no key_properties must be defined (the `config['timestamp_column']`'s value can be used to get the most recent information).

*  Even if there is no `config['timestamp_column']` attribute in the SCHEMA sent to the Snowflake Target for a specific stream, it is added explicitly. Each RECORD has the timestamp of when it was received by the Target as a value.

*  Schema updates are supported, both in case the schema of an entity changes in a future run but also in case we receive more than one SCHEMA messages for a specific stream in the same execution of `target-snowflake`.

    When a SCHEMA message for a stream is received, `target-snowflake` checks in the Snowflake Database provided in the config whether there is already a table for the entity defined by the stream.
    * If there is no such table (or even schema), they are created.
    * If there is already a table for that entity, `target-snowflake` creates a diff in order to check if new attributes must be added to the table or existing ones have their type updated and moves forward with making the changes.

    Rules followed:
    1. Only support type upgrades (e.g. STRING -> VARCHAR) for existing columns
    2. If a not supported type update is requested (e.g. float --> int), then an exception is raised.
    2. Never drop columns, only UPDATE existing ones or ADD new ones

    There is a limitation in Snowflake that really limits the possible transitions:

    https://docs.snowflake.net/manuals/sql-reference/sql/alter-table-column.html

    > In `ALTER TABLE … ALTER COLUMN <col1_name> SET DATA TYPE <type>`, when setting the TYPE for a column, the specified type must be a text data type (VARCHAR, STRING, TEXT, etc.).

    > Also, TYPE can be used only to increase the length of a text column.

    That means that no (INT --> FLOAT) or (INT --> STRING), etc type upgrades are allowed by Snowflake.

    Together with the fact that `target-snowflake` (wich follows the snowflake.sqlalchemy rules) uses the maximum type length for all strings (VARCHAR(16777216)), at the moment no meaningful data type upgrades are supported.

    But there is full support in `target-snowflake` for data type upgrades in case they are supported by Snowflake in the future. Additional `allowed_type_transitions` can be added in SnowflakeLoader as Snowflake may evolve its implementation in the future.

*  We unnest Nested JSON Data Structures and follow a `[object_name]__[property_name]` approach similar to [what Stitch platform also does](https://www.stitchdata.com/docs/data-structure/nested-data-structures-row-count-impact).

*  At the moment we do not deconstruct nested arrays. Arrays are stored as STRINGs with the relevant JSON representation stored as is. e.g. "['banana','apple']"

    In the future we may support as an option deconstructing ARRAYs and importing that data to sub-tables in an approach similar to [Stitch platform's Deconstruction of nested arrays](https://www.stitchdata.com/docs/data-structure/nested-data-structures-row-count-impact#deconstructing-nested-structures)

    For an explanation of why we are not using Snowflake's VARIANT, ARRAY or OBJECT types, check the following Section.


## Note on Snowflake's semi-structured data types

At the moment there is no proper support for Snowflake's semi-structured data types (VARIANT, ARRAY and OBJECT) in snowflake.sqlalchemy and no support for using sqlalchemy.types.JSON (`AttributeError: 'SnowflakeTypeCompiler' object has no attribute 'visit_JSON'`).

For more details check comment in [snowflakedb/snowflake-sqlalchemy test/test_semi_structured_datatypes.py#L55](https://github.com/snowflakedb/snowflake-sqlalchemy/blob/master/test/test_semi_structured_datatypes.py#L55) and followup insert tests

>  Semi-structured data cannot be inserted by INSERT VALUES. Instead, INSERT SELECT must be used. The fix should be either 1) SQLAlchemy dialect transforms INSERT statement or 2) Snwoflake DB supports INSERT VALUES for semi-structured data types. No ETA for this fix.

Until it is fixed, we are going to store semi-structured data (mainly JSON arrays) as strings and depend on a transformation step to convert those strings to proper snowflake VARIANT or ARRAY types.

For example, by using [dbt](https://www.getdbt.com/), we can store the original JSON array as string and then transform it to the Snowflake ARRAY type by using TO_ARRAY() on top of parse_json(). As an example using the TEST_STRINGS_IN_ARRAYS table created by target-snowflake's tests, we can and treat it as a proper Snowflake ARRAY:

```
SELECT TO_ARRAY(parse_json(STRINGS)), array_slice(TO_ARRAY(parse_json(STRINGS)), 0,1)
FROM "RAW"."TARGET_SNOWFLAKE_DEMO_DATA"."TEST_STRINGS_IN_ARRAYS";
```
