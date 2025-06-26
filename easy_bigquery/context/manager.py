from typing import Any, List, Literal, Optional

import pandas as pd
from google.cloud import bigquery as bq

from easy_bigquery.connector.connector import BigQueryConnector
from easy_bigquery.workers.fetcher import BigQueryFetcher
from easy_bigquery.workers.pusher import BigQueryPusher


class BigQueryManager:
    """
    A high-level context manager for BigQuery operations.

    This is the primary entry point for most users. It simplifies all
    interactions with BigQuery by providing an easy-to-use context
    manager (`with` statement) that handles the connection lifecycle
    automatically. Internally, it orchestrates the Connector, Fetcher,
    and Pusher classes.

    Attributes:
        connector (BigQueryConnector): The underlying connector instance.
        fetcher (Optional[BigQueryFetcher]): The fetcher instance,
            available after the context is entered.
        pusher (Optional[BigQueryPusher]): The pusher instance,
            available after the context is entered.

    Example:
        ```python
        import pandas as pd

        from easy_bigquery.context.manager import BigQueryManager

        # Using the Manager as a context manager handles all
        # connection and disconnection logic automatically.
        with BigQueryManager() as bq:
            # 1. Fetch data from a public dataset.
            sql = f'SELECT * FROM {bq.connector.project_id}.{bq.connector.dataset}.{bq.connector.table} LIMIT 15'
            df_fetched = bq.fetch(sql)
            print(df_fetched.head())

            # 2. Push a new DataFrame to your dataset.
            data = {'user_id': [100], 'status': ['active']}
            df_to_push = pd.DataFrame(data)
            table_name = 'test_table'
            bq.push(
                df=df_to_push,
                project_id=bq.connector.project_id,
                dataset=bq.connector.dataset,
                table=table_name,
                write_disposition='WRITE_APPEND',
            )
        ```
    """

    def __init__(self, **kwargs: Any):
        """
        Initializes the manager by creating a connector.

        Args:
            **kwargs: Keyword arguments to be passed down to the
                `BigQueryConnector` constructor (e.g., `project_id`).
        """
        self.connector = BigQueryConnector(**kwargs)
        self.fetcher: Optional[BigQueryFetcher] = None
        self.pusher: Optional[BigQueryPusher] = None

    def __enter__(self) -> 'BigQueryManager':
        """Establishes connection and initializes service classes."""
        self.connector.connect()
        self.fetcher = BigQueryFetcher(self.connector)
        self.pusher = BigQueryPusher(self.connector)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Closes the connection."""
        self.connector.close()

    def fetch(self, query: str, **kwargs: Any) -> pd.DataFrame:
        """
        High-level method to fetch data. Delegates to BigQueryFetcher.

        Args:
            query: The SQL query to execute.
            **kwargs: Additional arguments for the fetcher.

        Returns:
            A pandas DataFrame with the query results.
        """
        if not self.fetcher:
            raise ConnectionError('Manager context is not active.')
        return self.fetcher.fetch(query, **kwargs)

    def push(
        self,
        df: pd.DataFrame,
        project_id: Optional[str] = None,
        dataset: Optional[str] = None,
        table: Optional[str] = None,
        schema: Optional[List[bq.SchemaField]] = None,
        write_disposition: Literal[
            'WRITE_TRUNCATE',
            'WRITE_APPEND',
            'WRITE_EMPTY',
            'WRITE_DISPOSITION_UNSPECIFIED',
            'WRITE_TRUNCATE_DATA',
        ] = 'WRITE_APPEND',
    ) -> None:
        """
        High-level method to push data. Delegates to BigQueryPusher.

        Args:
            df: The pandas DataFrame to upload.
            project_id: Optional GCP project ID. Default value comes from
                environment variables.
            dataset: Optional GCP dataset name. Default value comes from
                environment variables.
            table: Optional GCP table name. Default value comes from
                environment variables.
            schema: Optional list of `SchemaField` objects.
            write_disposition: Write mode ('WRITE_TRUNCATE', 'WRITE_APPEND',
                'WRITE_EMPTY', 'WRITE_DISPOSITION_UNSPECIFIED',
                'WRITE_TRUNCATE_DATA'. Defaults to 'WRITE_APPEND').
        """
        if not self.pusher:
            raise ConnectionError('Manager context is not active.')
        self.pusher.push(
            df,
            project_id,
            dataset,
            table,
            schema,
            write_disposition,
        )
