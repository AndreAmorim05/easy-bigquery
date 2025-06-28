import pytest
from google.cloud import bigquery as bq

from easy_bigquery.workers.pusher import BigQueryPusher


def test_pusher_initialization_success(mock_connector_tuple):
    """Test successful BigQueryPusher initialization with an active connector."""
    # An active connection is required before creating the Pusher.
    connector, _ = mock_connector_tuple
    connector.connect()

    pusher = BigQueryPusher(connector)

    assert pusher.connector is connector
    assert pusher.connector.client is not None


def test_pusher_initialization_fails_if_not_connected(mock_connector_tuple):
    """Test that initialization fails if the connector is inactive."""
    connector, _ = mock_connector_tuple
    # Note that connector.connect() is intentionally not called.

    with pytest.raises(
        ConnectionError, match='Connector must be connected first.'
    ):
        BigQueryPusher(connector)


def test_push_success_with_defaults(mock_connector_tuple, sample_dataframe):
    """Test the push happy path, using default parameters and connector data."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    pusher = BigQueryPusher(connector)

    # Configure the mock to simulate a successful job.
    load_job_mock = mocks[
        'client_instance'
    ].load_table_from_dataframe.return_value
    load_job_mock.errors = None
    load_job_mock.output_rows = len(sample_dataframe)

    pusher.push(df=sample_dataframe)

    # Verify that the main load method was called once.
    mocks['client_instance'].load_table_from_dataframe.assert_called_once()

    # Inspect the call arguments to validate the details.
    _, call_kwargs = mocks[
        'client_instance'
    ].load_table_from_dataframe.call_args

    # Validate that the destination and job config use the correct defaults.
    expected_destination = (
        f'{connector.project_id}.{connector.dataset}.{connector.table}'
    )
    assert call_kwargs['destination'] == expected_destination
    job_config = call_kwargs['job_config']
    assert job_config.write_disposition == 'WRITE_APPEND'  # Default
    assert job_config.autodetect is True  # Default when no schema is given

    # Verify that the job result was awaited.
    load_job_mock.result.assert_called_once()


def test_push_with_explicit_parameters(mock_connector_tuple, sample_dataframe):
    """Test that explicit parameters override connector defaults."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    pusher = BigQueryPusher(connector)
    load_job_mock = mocks[
        'client_instance'
    ].load_table_from_dataframe.return_value
    load_job_mock.errors = None

    pusher.push(
        df=sample_dataframe,
        project_id='other-project',
        dataset='other_dataset',
        table='other_table',
        write_disposition='WRITE_TRUNCATE',
    )

    # Verify that the explicit parameters were used in the job call.
    call_kwargs = mocks[
        'client_instance'
    ].load_table_from_dataframe.call_args.kwargs
    assert (
        call_kwargs['destination'] == 'other-project.other_dataset.other_table'
    )
    assert call_kwargs['job_config'].write_disposition == 'WRITE_TRUNCATE'


def test_push_with_explicit_schema(mock_connector_tuple, sample_dataframe):
    """Test that providing a schema disables 'autodetect'."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    pusher = BigQueryPusher(connector)
    load_job_mock = mocks[
        'client_instance'
    ].load_table_from_dataframe.return_value
    load_job_mock.errors = None

    # Create a mock schema to be passed to the push method.
    mock_schema = [
        bq.SchemaField('col1', 'INTEGER'),
        bq.SchemaField('col2', 'STRING'),
    ]

    pusher.push(df=sample_dataframe, schema=mock_schema)

    # Verify that autodetect is False and the provided schema is used.
    job_config = mocks[
        'client_instance'
    ].load_table_from_dataframe.call_args.kwargs['job_config']

    assert job_config.autodetect is False
    assert job_config.schema == mock_schema


def test_push_raises_runtime_error_on_job_failure(
    mock_connector_tuple, sample_dataframe
):
    """Test that a RuntimeError is raised when the BQ job reports errors."""
    connector, mocks = mock_connector_tuple
    connector.connect()
    pusher = BigQueryPusher(connector)

    # Configure the mock to simulate a job with errors.
    load_job_mock = mocks[
        'client_instance'
    ].load_table_from_dataframe.return_value
    load_job_mock.errors = [
        {'reason': 'invalid', 'message': 'Invalid field name'}
    ]

    with pytest.raises(RuntimeError, match='BigQuery load job failed.'):
        pusher.push(df=sample_dataframe)

    # Verify that the job result was awaited even on failure.
    load_job_mock.result.assert_called_once()


def test_push_raises_error_if_client_disappears(
    mock_connector_tuple, sample_dataframe
):
    """Test that a RuntimeError is raised if the client is unavailable."""
    connector, _ = mock_connector_tuple
    connector.connect()
    pusher = BigQueryPusher(connector)

    # Simulate the client "disappearing" after initialization.
    pusher.connector.client = None

    with pytest.raises(RuntimeError, match='BigQuery client not initialized.'):
        pusher.push(df=sample_dataframe)
