"""
Integration tests for Easy BigQuery.

These tests require real BigQuery credentials and will make actual API calls.
They are designed to test the full integration with Google BigQuery.
"""

import os
from datetime import datetime

import pandas as pd  # type: ignore
import pytest  # type: ignore

from easy_bigquery import BQConnector, BQManager
from easy_bigquery.workers import FetchWorker, PushWorker


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("BQ_JSON_CREDENTIALS"),
    reason="Integration tests require BQ_JSON_CREDENTIALS environment variable",
)
class TestIntegration:
    """Integration tests for Easy BigQuery."""

    @pytest.fixture
    def test_table_name(self):
        """Generate a unique test table name."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"test_integration_{timestamp}"

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "charlie@example.com",
                    "diana@example.com",
                    "eve@example.com",
                ],
                "status": ["active", "inactive", "active", "active", "inactive"],
                "created_at": pd.Timestamp.now(),
            }
        )

    def test_bq_manager_context_manager(self, test_table_name, sample_dataframe):
        """Test BQManager as a context manager."""
        with BQManager() as bq:
            # Test fetch from public dataset
            df = bq.fetch(
                """
                SELECT name, state
                FROM `bigquery-public-data.usa_names.usa_1910_current`
                LIMIT 5"""
            )
            assert len(df) == 5
            assert "name" in df.columns
            assert "state" in df.columns

            # Test push to test table
            bq.push(
                df=sample_dataframe,
                table=test_table_name,
                write_disposition="WRITE_TRUNCATE",
            )

            # Verify data was uploaded
            result_df = bq.fetch(f"SELECT * FROM {test_table_name}")
            assert len(result_df) == 5
            assert "id" in result_df.columns
            assert "name" in result_df.columns

    def test_manual_connection_management(self, test_table_name, sample_dataframe):
        """Test manual connection management with connector and workers."""
        connector = BQConnector()

        try:
            connector.connect()

            # Test fetch worker
            fetcher = FetchWorker(connector)
            df = fetcher.fetch(
                """
                SELECT name, state
                FROM `bigquery-public-data.usa_names.usa_1910_current`
                LIMIT 3
                """
            )
            assert len(df) == 3

            # Test push worker
            pusher = PushWorker(connector)
            pusher.push(
                df=sample_dataframe,
                table=test_table_name,
                write_disposition="WRITE_TRUNCATE",
            )

            # Verify upload
            result_df = fetcher.fetch(f"SELECT * FROM {test_table_name}")
            assert len(result_df) == 5

        finally:
            connector.close()

    def test_different_write_dispositions(self, test_table_name, sample_dataframe):
        """Test different write dispositions."""
        with BQManager() as bq:
            # Test WRITE_TRUNCATE
            bq.push(
                df=sample_dataframe,
                table=test_table_name,
                write_disposition="WRITE_TRUNCATE",
            )

            result_df = bq.fetch(f"SELECT * FROM {test_table_name}")
            assert len(result_df) == 5

            # Test WRITE_APPEND
            new_data = pd.DataFrame(
                {
                    "id": [6, 7],
                    "name": ["Frank", "Grace"],
                    "email": ["frank@example.com", "grace@example.com"],
                    "status": ["active", "active"],
                    "created_at": pd.Timestamp.now(),
                }
            )

            bq.push(
                df=new_data, table=test_table_name, write_disposition="WRITE_APPEND"
            )

            result_df = bq.fetch(f"SELECT * FROM {test_table_name}")
            assert len(result_df) == 7

    def test_custom_schema(self, test_table_name):
        """Test uploading with custom schema."""
        from google.cloud import bigquery as bq # type: ignore

        # Create data with specific types
        data = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "is_active": [True, False, True],
                "score": [95.5, 87.2, 92.1],
            }
        )

        # Define custom schema
        schema = [
            bq.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
            bq.SchemaField("name", "STRING", mode="REQUIRED"),
            bq.SchemaField("is_active", "BOOLEAN", mode="REQUIRED"),
            bq.SchemaField("score", "FLOAT", mode="NULLABLE"),
        ]

        with BQManager() as bq_manager:
            bq_manager.push(
                df=data,
                table=test_table_name,
                schema=schema,
                write_disposition="WRITE_TRUNCATE",
            )

            # Verify upload
            result_df = bq_manager.fetch(f"SELECT * FROM {test_table_name}")
            assert len(result_df) == 3
            assert result_df["user_id"].dtype == "int64"
            assert result_df["is_active"].dtype == "bool"

    def test_large_dataset_handling(self, test_table_name):
        """Test handling of larger datasets."""
        # Create a larger dataset
        large_data = pd.DataFrame(
            {
                "id": range(1000),
                "value": [f"value_{i}" for i in range(1000)],
                "timestamp": pd.Timestamp.now(),
            }
        )

        with BQManager() as bq:
            # Upload large dataset
            bq.push(
                df=large_data, table=test_table_name, write_disposition="WRITE_TRUNCATE"
            )

            # Verify upload
            result_df = bq.fetch(f"SELECT COUNT(*) as count FROM {test_table_name}")
            assert result_df.iloc[0]["count"] == 1000

    def test_error_handling(self):
        """Test error handling for invalid operations."""
        with BQManager() as bq:
            # Test invalid SQL
            with pytest.raises(Exception):
                bq.fetch("SELECT * FROM non_existent_table")

            # Test invalid table name
            with pytest.raises(Exception):
                bq.push(
                    df=pd.DataFrame({"test": [1]}),
                    table="invalid/table/name",
                    write_disposition="WRITE_TRUNCATE",
                )

    def test_connection_cleanup(self):
        """Test that connections are properly cleaned up."""
        connector = BQConnector()

        # Verify initial state
        assert connector.client is None
        assert connector.bq_storage is None

        connector.connect()

        # Verify connected state
        assert connector.client is not None
        assert connector.bq_storage is not None

        connector.close()

        # Verify cleaned up state
        assert connector.client is None
        assert connector.bq_storage is None


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("BQ_JSON_CREDENTIALS"),
    reason="Integration tests require BQ_JSON_CREDENTIALS environment variable",
)
class TestPerformance:
    """Performance tests for Easy BigQuery."""

    def test_storage_api_performance(self):
        """Test performance difference between Storage API and standard API."""
        import time

        with BQManager() as bq:
            # Test with Storage API (default)
            start_time = time.time()
            df_storage = bq.fetch(
                """
                SELECT *
                FROM `bigquery-public-data.usa_names.usa_1910_current`
                LIMIT 10000
                """,
                use_storage_api=True,
            )
            storage_time = time.time() - start_time

            # Test without Storage API
            start_time = time.time()
            df_standard = bq.fetch(
                """
                SELECT * FROM `bigquery-public-data.usa_names.usa_1910_current`
                LIMIT 10000
                """,
                use_storage_api=False,
            )
            standard_time = time.time() - start_time

            # Verify same results
            assert len(df_storage) == len(df_standard)
            assert df_storage.equals(df_standard)

            # Storage API should be faster (but not always guaranteed)
            print(f"Storage API time: {storage_time:.2f}s")
            print(f"Standard API time: {standard_time:.2f}s")
