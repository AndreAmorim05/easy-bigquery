import pandas as pd
import pytest

from easy_bigquery.workers.fetch import FetchWorker


def test_fetcher_initialization_success(mock_connector_tuple):
    """Test successful FetchWorker initialization with an active connector."""
    # An active connection is required before creating the Fetcher.
    connector, _ = mock_connector_tuple
    connector.connect()

    fetcher = FetchWorker(connector)

    assert fetcher.connector is connector
    assert fetcher.connector.client is not None


def test_fetcher_initialization_fails_if_not_connected(mock_connector_tuple):
    """Test that initialization fails if the connector is inactive."""
    connector, _ = mock_connector_tuple
    # Note that connector.connect() is intentionally not called.

    with pytest.raises(
        ConnectionError, match='Connector must be connected first.'
    ):
        FetchWorker(connector)


def test_fetch_with_storage_api_success(
    mock_connector_tuple, sample_dataframe
):
    """Test the fetch happy path, using the Storage API by default."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    fetcher = FetchWorker(connector)
    sql_query = 'SELECT * FROM my_table'

    # Mock the query job to return the sample DataFrame.
    mocks[
        'client_instance'
    ].query.return_value.to_dataframe.return_value = sample_dataframe

    result_df = fetcher.fetch(sql_query)

    # Verify that the correct SQL query was executed.
    mocks['client_instance'].query.assert_called_once_with(sql_query)

    # Verify that to_dataframe was called with the BQ Storage client.
    job_mock = mocks['client_instance'].query.return_value
    job_mock.to_dataframe.assert_called_once_with(
        bqstorage_client=mocks['storage_instance']
    )

    # Verify that the returned DataFrame matches the expected one.
    pd.testing.assert_frame_equal(result_df, sample_dataframe)


def test_fetch_without_storage_api(mock_connector_tuple, sample_dataframe):
    """Test fetching data with the Storage API disabled."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    fetcher = FetchWorker(connector)
    sql_query = 'SELECT name FROM my_table'
    mocks[
        'client_instance'
    ].query.return_value.to_dataframe.return_value = sample_dataframe

    result_df = fetcher.fetch(sql_query, use_storage_api=False)

    # Verify that the correct SQL query was executed.
    mocks['client_instance'].query.assert_called_once_with(sql_query)

    # Verify to_dataframe was called without the BQ Storage client.
    job_mock = mocks['client_instance'].query.return_value
    job_mock.to_dataframe.assert_called_once_with(bqstorage_client=None)
    pd.testing.assert_frame_equal(result_df, sample_dataframe)


def test_fetch_passes_kwargs_to_dataframe(mock_connector_tuple):
    """Test if extra kwargs are correctly passed to to_dataframe()."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    fetcher = FetchWorker(connector)

    fetcher.fetch('SELECT 1', progress_bar_type='tqdm')

    # Verify that kwargs are passed through to the to_dataframe call.
    job_mock = mocks['client_instance'].query.return_value
    job_mock.to_dataframe.assert_called_once_with(
        bqstorage_client=mocks['storage_instance'],
        progress_bar_type='tqdm',
    )


def test_fetch_raises_runtime_error_if_client_disappears(
    mock_connector_tuple,
):
    """Test that fetch raises RuntimeError if the client becomes unavailable."""
    connector, _ = mock_connector_tuple
    connector.connect()
    fetcher = FetchWorker(connector)

    # Simulate an unexpected condition where the client disappears.
    fetcher.connector.client = None

    with pytest.raises(
        RuntimeError, match='BigQuery client is not available.'
    ):
        fetcher.fetch('SELECT 1')
