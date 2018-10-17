import json
import pytest


def pytest_addoption(parser):
    parser.addoption("--config", help="Config file", required=True)


@pytest.fixture(scope="session")
def config(request):
    config_file = request.config.getoption("--config")

    if config_file:
        with open(config_file) as input:
            config = json.load(input)
    else:
        raise Exception("No config file provided")

    config["timestamp_column"] = config.get("timestamp_column", "__loaded_at")

    return config
