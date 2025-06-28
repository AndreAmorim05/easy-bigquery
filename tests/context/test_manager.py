import pytest

from easy_bigquery.context.manager import BigQueryManager


def test_manager_initialization(mocked_manager_dependencies):
    """Test if the Manager correctly initializes the Connector, passing kwargs."""
    mocks = mocked_manager_dependencies

    manager = BigQueryManager(project_id='my-custom-project')

    # Verify that the Connector was instantiated with the correct arguments.
    mocks['connector_class'].assert_called_once_with(
        project_id='my-custom-project'
    )
    assert manager.connector is mocks['connector_instance']

    # Ensure workers have not been created upon initialization.
    assert manager.fetcher is None
    assert manager.pusher is None


def test_manager_context_entry_and_exit(mocked_manager_dependencies):
    """Test the context manager lifecycle: __enter__ and __exit__."""
    mocks = mocked_manager_dependencies
    manager = BigQueryManager()

    with manager as bq_manager:
        # --- __enter__ assertions ---
        # Verify the connector was activated.
        mocks['connector_instance'].connect.assert_called_once()

        # Verify workers were instantiated with the correct connector.
        mocks['fetcher_class'].assert_called_once_with(
            mocks['connector_instance']
        )
        assert bq_manager.fetcher is mocks['fetcher_instance']
        mocks['pusher_class'].assert_called_once_with(
            mocks['connector_instance']
        )
        assert bq_manager.pusher is mocks['pusher_instance']

        # Verify the context returns the manager instance itself.
        assert bq_manager is manager

    # --- __exit__ assertions ---
    # The __exit__ method is called automatically after the 'with' block.
    # Verify the connection was closed.
    mocks['connector_instance'].close.assert_called_once()


def test_manager_delegates_fetch_call(mocked_manager_dependencies):
    """Test if the Manager's fetch method delegates the call to the Fetcher."""
    mocks = mocked_manager_dependencies
    manager = BigQueryManager()

    with manager:
        sql_query = 'SELECT * FROM my_table'
        manager.fetch(sql_query, use_storage_api=False)

        # Verify the call was delegated to the fetcher instance.
        mocks['fetcher_instance'].fetch.assert_called_once_with(
            sql_query, use_storage_api=False
        )


def test_manager_delegates_push_call(
    mocked_manager_dependencies, sample_dataframe
):
    """Test if the Manager's push method delegates the call to the Pusher."""
    mocks = mocked_manager_dependencies
    manager = BigQueryManager()

    with manager:
        manager.push(
            df=sample_dataframe,
            table='target_table',
            write_disposition='WRITE_TRUNCATE',
        )

        # Verify the call was delegated to the pusher instance.
        mocks['pusher_instance'].push.assert_called_once_with(
            sample_dataframe,
            None,
            None,
            'target_table',
            None,
            'WRITE_TRUNCATE',
        )


def test_fetch_or_push_fails_outside_context(
    mocked_manager_dependencies, sample_dataframe
):
    """Test that calling fetch/push outside a `with` context raises an error."""
    manager = BigQueryManager()

    # Test fetch call outside context.
    with pytest.raises(
        ConnectionError, match='Manager context is not active.'
    ):
        manager.fetch('SELECT 1')

    # Test push call outside context.
    with pytest.raises(
        ConnectionError, match='Manager context is not active.'
    ):
        manager.push(df=sample_dataframe)
