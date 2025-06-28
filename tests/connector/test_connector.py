import json

from easy_bigquery.connector.connector import BQConnector


def test_connector_initialization(mock_connector_tuple):
    """Test if the connector is initialized with the correct state."""
    connector, mocks = mock_connector_tuple

    # Assert the initial state of the connector's attributes.
    assert connector.project_id == 'test-project'
    assert connector.dataset == 'test_dataset'
    assert connector.table == 'test_table'
    assert connector.client is None
    assert connector.bq_storage is None

    # Verify that no client instantiation has occurred yet.
    mocks['client_class'].assert_not_called()
    mocks['storage_class'].assert_not_called()


def test_connector_connect(mock_connector_tuple):
    """Test if the connect() method instantiates and assigns clients correctly."""
    connector, mocks = mock_connector_tuple

    # Trigger the connection method.
    connector.connect()

    # Verify that credentials are loaded correctly.
    mock_creds_info = json.loads(
        '{"type": "service_account", "project_id": "test-project"}'
    )
    mocks['from_creds'].assert_called_once_with(info=mock_creds_info)

    # Verify that both BigQuery and BigQuery Storage clients are created.
    mocks['client_class'].assert_called_once_with(
        credentials=mocks['credentials'], project='test-project'
    )
    mocks['storage_class'].assert_called_once_with(
        credentials=mocks['credentials']
    )

    # Check that client instances are assigned to the connector.
    assert connector.client is mocks['client_instance']
    assert connector.bq_storage is mocks['storage_instance']


def test_connector_close(mock_connector_tuple):
    """Test if the close() method cleans up clients and transport."""
    connector, mocks = mock_connector_tuple

    # Set up the state by connecting first.
    connector.connect()
    assert connector.client is not None

    # Trigger the close method.
    connector.close()

    # Verify that the transport is closed and clients are cleared.
    mocks['storage_instance'].transport.close.assert_called_once()
    assert connector.client is None
    assert connector.bq_storage is None
