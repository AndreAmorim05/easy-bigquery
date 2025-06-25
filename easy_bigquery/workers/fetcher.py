from typing import Any

import pandas as pd

from easy_bigquery.connector.connector import BigQueryConnector
from easy_bigquery.logger import logger


class BigQueryFetcher:
    """
    Handles fetching data from BigQuery into pandas DataFrames.

    This class encapsulates the logic for executing SQL queries. It
    requires an active, pre-configured `BigQueryConnector` instance to
    perform its operations. This design decouples the fetch logic from
    the connection management.

    Attributes:
        connector (BigQueryConnector): An active and connected
            BigQueryConnector instance.

    Example:
        ```python
        from easy_bigquery.connector.connector import BigQueryConnector
        from easy_bigquery.fetcher.fetcher import BigQueryFetcher
        sql = 'SELECT name, state FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 3'
        connector = BigQueryConnector()
        try:
            connector.connect()
            # The fetcher needs an active connector to work.
            fetcher = BigQueryFetcher(connector)
            df = fetcher.fetch(sql)
            print('Fetched DataFrame:')
            print(df)
        finally:
            connector.close()
        ```
    """

    def __init__(self, connector: BigQueryConnector):
        """
        Initializes the BigQueryFetcher.

        Args:
            connector: An initialized and connected `BigQueryConnector`
                instance.

        Raises:
            ConnectionError: If the provided connector is not active.
        """
        if not connector.client or not connector.bq_storage:
            raise ConnectionError('Connector must be connected first.')
        self.connector = connector

    def fetch(
        self, query: str, use_storage_api: bool = True, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a DataFrame.

        Args:
            query: The SQL query string to execute.
            use_storage_api: If True, uses the faster BigQuery Storage
                API for downloading results. Defaults to True.
            **kwargs: Additional keyword arguments to pass to the
                `to_dataframe()` method of the underlying query job.

        Returns:
            A pandas DataFrame containing the query results.

        Raises:
            RuntimeError: If the BigQuery client is not available.
        """
        if not self.connector.client:
            raise RuntimeError('BigQuery client is not available.')

        logger.info(f'Executing query with storage_api={use_storage_api}')
        job = self.connector.client.query(query)

        df = job.to_dataframe(
            bqstorage_client=(
                self.connector.bq_storage if use_storage_api else None
            ),
            **kwargs,
        )
        logger.info(f'Query returned {len(df)} rows.')
        return df
