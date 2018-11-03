import json
import singer
import sys

from datetime import datetime
from jsonschema import ValidationError, Draft4Validator, FormatChecker
from typing import Dict, List

from target_snowflake.utils.singer_target_utils import (
    flatten_record,
    generate_sqlalchemy_table,
)
from target_snowflake.snowflake_loader import SnowflakeLoader


class RecordBuffer(list):
    def add_record(self, record: Dict):
        self.append(record)

    def values(self):
        return self


class UniqueRecordBuffer(dict):
    def __init__(self, key_func=lambda x: x):
        self.key = key_func

    def add_record(self, record: Dict):
        self[self.key(record)] = record

    def values(self):
        return list(super().values())

    def __iter__(self):
        for record in self.values():
            yield record


class TargetSnowflake:
    def __init__(self, config: Dict) -> None:
        # Store the Config so that we can use it to initiate Snowflake Loaders
        #  for various tables
        self.config: Dict = config
        self.batch_size = int(config.get("batch_size", 5000))
        self.timestamp_column = config.get("timestamp_column", "__loaded_at")

        # Store the latest state so that we return it when the loading completes
        self.state = None

        # Keep track of the streams we have schemas for.
        # A tap sending a record without previously describing its schema is not
        #  properly following the Singer.io Spec
        # The schemas variable is only used for lookups as the SnowflakeLoader
        #  for that stream with all the schema info and the connection options
        #  is stored for each stream in loaders
        self.schemas: List = []
        self.loaders: Dict = {}

        # Also keep track of a template empty record for each stream in order
        #  to map all incoming records against and normalize them to use their
        #  fully defined schema
        self.template_records: Dict = {}

        # The key_properties has the keys for each stream to enable quick
        #  lookups during schema validation of each received record
        #  (all keys should be there even if they are not marked as required)
        self.key_properties: Dict = {}

        # For each stream, also keep a schema JSON Schema validator to validate
        #  new records against
        self.validators: Dict = {}

        # Cache the records for each stream in rows[stream]
        # When the cache reaches the batch_size or when the tap stops
        #  sending data, we flush the cached records (i.e. send them in batch to
        #  Snowflake). This is important for performance: we don't want to send
        #  an insert with each record received.
        self.rows: Dict = {}

        self.logger = singer.get_logger()

    def extract_keys(self, stream: str, record: Dict):
        return tuple(record[key] for key in self.key_properties[stream])

    def process_line(self, line: str) -> None:
        """
        Process a Singer.io Message, which is provided in a single line
        """
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            self.logger.error("Unable to parse:\n{}".format(line))
            raise

        if "type" not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )

            stream = o["stream"]
            if stream not in self.schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        stream
                    )
                )

            # Validate record against the schema for that stream
            self.schema_validation(stream, o["record"], self.key_properties[stream])

            # Flatten the record
            flat_record = flatten_record(o["record"])

            # Add an `timestamp_column` timestamp for the record
            if self.timestamp_column not in flat_record:
                flat_record[self.timestamp_column] = datetime.utcnow()

            # Normalize the record to make sure it follows the full schema defined
            new_record = self.template_records[stream].copy()
            new_record.update(flat_record)

            # Store the record so that we can load in batch_size batches
            self.rows[stream].add_record(new_record)

            # If the batch_size has been reached for this stream, flush the records
            if len(self.rows[stream]) >= self.batch_size:
                self.flush_records(stream)

            self.state = None
        elif t == "STATE":
            self.logger.debug("Setting state to {}".format(o["value"]))
            self.state = o["value"]
        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )

            stream = o["stream"]

            if stream in self.schemas:
                # We received a new Schema message for a stream that already
                #  has a Schema defined.
                # Flush the cached records as we may have an updated Schema
                #  going forward that will be incompatible with the current one
                self.flush_records(stream)
            else:
                # The Schema message is for a newly encountered stream
                # Record that the schema for this stream has been received
                self.schemas.append(stream)

            # Add a validator based on the received JSON Schema
            self.validators[stream] = Draft4Validator(
                o["schema"], format_checker=FormatChecker()
            )

            # We could live without it for append only use cases without a key,
            #  but it is part of the Singer.io SPEC
            if "key_properties" not in o:
                raise Exception("key_properties field is required")

            key_properties = o["key_properties"]

            # Store the Key properties for quick lookups during record validation
            self.key_properties[stream] = key_properties

            # Generate an sqlalchemy Table based on the info received
            # It is used to store and access all the schema information
            #  in a structured way
            sqlalchemy_table = generate_sqlalchemy_table(
                stream, key_properties, o["schema"], self.timestamp_column
            )

            # Create a SnowflakeLoader for that sqlalchemy Table and
            #  run schema_apply() to create the Schema and/or Table if they
            #  are not there.
            loader = SnowflakeLoader(table=sqlalchemy_table, config=self.config)
            loader.schema_apply()

            # This buffering makes sure that if we receive multiple rows that
            #  would violate the `key_properties` uniqueness,
            #  only the last one will be kept.
            if key_properties:
                self.rows[stream] = UniqueRecordBuffer(
                    lambda record: self.extract_keys(stream, record)
                )
            else:
                self.rows[stream] = RecordBuffer()

            # Keep a template empty record for each stream in order to map
            #  all incoming records against
            self.template_records[stream] = loader.empty_record()

            # Keep the loader in loaders[stream] to be used for loading the
            #  records received for that stream.
            self.loaders[stream] = loader
        elif t == "ACTIVATE_VERSION":
            # No support for that type of message yet
            self.logger.warn("ACTIVATE_VERSION message")
        else:
            raise Exception(
                "Unknown message type {} in message {}".format(o["type"], o)
            )

    def schema_validation(self, stream: str, record: Dict, keys: List) -> None:
        """
        Validate a record against the schema for its stream

        Checks that:
        1. The record follows the JSON schema of the SCHEMA message
        2. All the keys are present even if they are not market as required in
             the JSON schema
        """
        self.validators[stream].validate(record)

        if not keys:
            return

        for key in keys:
            if key not in record:
                raise ValidationError(f"Record {record} is missing key property {key}")

    def flush_all_cached_records(self) -> None:
        """
        Flush the records for any remaining streams that still have
        records cached (i.e. row_count < batch_size)
        """
        to_flush = (stream for (stream, rows) in self.rows.items() if len(rows))

        for stream in to_flush:
            self.flush_records(stream)

    def flush_records(self, stream: str) -> None:
        """
        Flush the cached records stored in rows[stream] for a specific stream.

        loaders[stream] has an initialized SnowflakeLoader for the table defined
        by the schema we have received for that stream.
        """

        # Load the data
        self.loaders[stream].load(self.rows[stream].values())

        # Clear the cached records and reset the counter for the stream
        self.rows[stream].clear()

    def emit_state(self) -> None:
        """
        Emit the current state to stdout
        """
        if self.state is not None:
            line = json.dumps(self.state)
            self.logger.debug("Emitting state {}".format(line))
            sys.stdout.write("{}\n".format(line))
            sys.stdout.flush()
