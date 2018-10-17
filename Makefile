.PHONY: test test-quick test-run build mypy

APP_NAME=target-snowflake

build:
	@echo "Building app..."
	@time docker build -t ${APP_NAME} .

test-full:
	@echo "Running full test suite..."
	@time docker run \
					-e SF_ACCOUNT -e SF_USER -e SF_PASSWORD -e SF_ROLE \
					-e SF_DATABASE -e SF_TEST_SCHEMA -e SF_WAREHOUSE \
					${APP_NAME} /bin/bash -c \
						"python target_snowflake/utils/config_generator.py ; \
						pytest tests/ -vv --config config.json"

test-quick:
	@echo 'Running tests with the "not slow" option on...'
	@time docker run \
					-e SF_ACCOUNT -e SF_USER -e SF_PASSWORD -e SF_ROLE \
					-e SF_DATABASE -e SF_TEST_SCHEMA -e SF_WAREHOUSE \
					${APP_NAME} /bin/bash -c \
						'python target_snowflake/utils/config_generator.py ; \
						pytest tests/ -vv --config config.json -m "not slow" '

test-run:
	@echo "Running target-snowflake with test data..."
	@time docker run \
					-e SF_ACCOUNT -e SF_USER -e SF_PASSWORD -e SF_ROLE \
					-e SF_DATABASE -e SF_TEST_SCHEMA -e SF_WAREHOUSE \
					${APP_NAME}  /bin/bash -c \
							"python target_snowflake/utils/config_generator.py ; \
							cat tests/data_files/user_location_data.stream | target-snowflake -c config.json ; \
							cat tests/data_files/array_data.stream | target-snowflake -c config.json"

mypy:
	@echo "Running mypy for type-checking..."
	@time docker run ${APP_NAME} mypy target_snowflake --ignore-missing-imports

lint:
	@echo "Running linter..."
	@time black target_snowflake tests
