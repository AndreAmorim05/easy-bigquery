from typing import List, Literal, Optional

import pandas as pd
from google.cloud import bigquery as bq

from easy_bigquery.connector.connector import BigQueryConnector
from easy_bigquery.logger import logger


class BigQueryPusher:
    """
    Handles pushing pandas DataFrames to a BigQuery table.

    This class encapsulates the logic for loading DataFrames into
    BigQuery. It requires an active, pre-configured `BigQueryConnector`
    instance to perform its operations, separating the push logic from
    connection management.

    Attributes:
        connector (BigQueryConnector): An active and connected
            BigQueryConnector instance.

    Example:
        ```python
        from easy_bigquery.connector.connector import BigQueryConnector
        from easy_bigquery.pusher.pusher import BigQueryPusher
        import pandas as pd

        # Create a sample DataFrame to upload.
        data = {'product_id': [101, 102], 'product_name': ['Gadget', 'Widget']}
        df_to_push = pd.DataFrame(data)

        connector = BigQueryConnector()
        try:
            connector.connect()
            # Define the destination table.
            table_id = f'{connector.dataset}.my_products_table'

            # The pusher needs an active connector.
            pusher = BigQueryPusher(connector)
            pusher.push(
                df=df_to_push,
                table_id=table_id,
                write_disposition='WRITE_TRUNCATE'
            )
            print(f'Successfully pushed data to {table_id}')
        finally:
            connector.close()
        ```
    """

    def __init__(self, connector: BigQueryConnector):
        """
        Initializes the BigQueryPusher.

        Args:
            connector: An initialized and connected `BigQueryConnector`
                instance.

        Raises:
            ConnectionError: If the provided connector is not active.
        """
        if not connector.client:
            raise ConnectionError('Connector must be connected first.')
        self.connector = connector

    def push(
        self,
        df: pd.DataFrame,
        project_id: str = None,
        dataset: str = None,
        table: str = None,
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
        Loads a DataFrame into a BigQuery table.

        Args:
            df: The pandas DataFrame to upload.
            table_id: The destination table ID in the format
                `dataset_id.table_id`.
            schema: An optional list of `bigquery.SchemaField` objects.
                If None, BigQuery will attempt to infer the schema.
                Defaults to None.
            write_disposition: Specifies the action if the table exists.
                Valid values are 'WRITE_TRUNCATE', 'WRITE_APPEND',
                'WRITE_EMPTY', 'WRITE_DISPOSITION_UNSPECIFIED', or
                'WRITE_TRUNCATE_DATA'. Defaults to 'WRITE_APPEND'
        """
        if not self.connector.client:
            raise RuntimeError('BigQuery client not initialized.')

        job_config = bq.LoadJobConfig(
            create_disposition=bq.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=write_disposition,
            autodetect=True if schema is None else False,
            schema=schema,
        )

        full_table_path = f'{project_id or self.connector.project_id}.{dataset or self.connector.dataset}.{table or self.connector.table}'
        logger.info(f'Loading {len(df)} rows to {full_table_path}...')

        load_job = self.connector.client.load_table_from_dataframe(
            dataframe=df, destination=full_table_path, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        if load_job.errors:
            logger.error(f'Load job failed: {load_job.errors}')
            raise RuntimeError('BigQuery load job failed.', load_job.errors)
        logger.info(f'Successfully loaded {load_job.output_rows} rows.')
