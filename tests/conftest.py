from unittest.mock import MagicMock

import pandas as pd
import pytest

from easy_bigquery.connector.connector import BigQueryConnector

# Define constants for mocking to ensure consistency across tests.
MOCK_PROJECT_ID = 'test-project'
MOCK_DATASET = 'test_dataset'
MOCK_TABLE = 'test_table'
MOCK_CREDS_INFO_STR = (
    '{"type": "service_account", "project_id": "test-project"}'
)


@pytest.fixture
def mock_connector_tuple(mocker):
    """
    Prepare a BigQueryConnector with mocked dependencies.

    This fixture patches the external dependencies of the BigQueryConnector
    and yields both the connector instance and a dictionary of the mocks.
    This gives the test full control to trigger methods (Act) and verify
    calls on the mocks (Assert).

    Yields:
        A tuple containing the connector instance and a dictionary of mocks.
    """
    # Patch all external classes used by the connector.
    mock_from_creds = mocker.patch(
        'easy_bigquery.connector.connector.service_account.Credentials'
        '.from_service_account_info'
    )
    mock_bq_client_class = mocker.patch(
        'easy_bigquery.connector.connector.bq.Client'
    )
    mock_storage_client_class = mocker.patch(
        'easy_bigquery.connector.connector.BigQueryReadClient'
    )

    # Prepare mock instances that the patched classes will return.
    mock_client_instance = MagicMock()
    mock_storage_instance = MagicMock()
    mock_credentials_instance = MagicMock()

    # Configure the patched classes to return the mock instances.
    mock_from_creds.return_value = mock_credentials_instance
    mock_bq_client_class.return_value = mock_client_instance
    mock_storage_client_class.return_value = mock_storage_instance

    # Instantiate the connector with test data. Note: .connect() is not called.
    connector = BigQueryConnector(
        project_id=MOCK_PROJECT_ID,
        credentials_info=MOCK_CREDS_INFO_STR,
        dataset=MOCK_DATASET,
        table=MOCK_TABLE,
    )

    # Group all mocks into a dictionary for easy access in tests.
    mocks = {
        'from_creds': mock_from_creds,
        'credentials': mock_credentials_instance,
        'client_class': mock_bq_client_class,
        'client_instance': mock_client_instance,
        'storage_class': mock_storage_client_class,
        'storage_instance': mock_storage_instance,
    }

    yield connector, mocks
    # Teardown (undoing patches) is handled automatically by mocker.


@pytest.fixture
def sample_dataframe():
    """Provide a sample pandas DataFrame for testing."""
    return pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})


@pytest.fixture
def mocked_manager_dependencies(mocker):
    """
    Mock the direct dependencies of the BigQueryManager.

    This fixture replaces the Connector, Fetcher, and Pusher classes
    within the manager's module. This allows testing the manager's
    orchestration logic without executing the actual code of these
    dependencies.

    Yields:
        A dictionary containing all created mocks.
    """
    # Patch the classes in the module where they are imported and used.
    mock_connector_class = mocker.patch(
        'easy_bigquery.context.manager.BigQueryConnector'
    )
    mock_fetcher_class = mocker.patch(
        'easy_bigquery.context.manager.BigQueryFetcher'
    )
    mock_pusher_class = mocker.patch(
        'easy_bigquery.context.manager.BigQueryPusher'
    )

    # Create mock instances to be returned by the mocked class constructors.
    mock_connector_instance = MagicMock()
    mock_fetcher_instance = MagicMock()
    mock_pusher_instance = MagicMock()

    mock_connector_class.return_value = mock_connector_instance
    mock_fetcher_class.return_value = mock_fetcher_instance
    mock_pusher_class.return_value = mock_pusher_instance

    # Group the mocks for easy access in tests.
    mocks = {
        'connector_class': mock_connector_class,
        'fetcher_class': mock_fetcher_class,
        'pusher_class': mock_pusher_class,
        'connector_instance': mock_connector_instance,
        'fetcher_instance': mock_fetcher_instance,
        'pusher_instance': mock_pusher_instance,
    }

    yield mocks
