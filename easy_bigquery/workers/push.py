from typing import List, Literal, Optional

import pandas as pd
from google.cloud import bigquery as bq

from easy_bigquery.connector.connector import BQConnector
from easy_bigquery.logger import logger


class PushWorker:
    """
    Handles pushing pandas DataFrames to a BigQuery table.

    This class encapsulates the logic for loading DataFrames into
    BigQuery. It requires an active, pre-configured `BQConnector`
    instance to perform its operations, separating the push logic from
    connection management.

    Attributes:
        connector (BQConnector): An active and connected
            BQConnector instance.

    Example:
        ```python
        import pandas as pd

        from easy_bigquery import BQConnector
        from easy_bigquery.workers import PushWorker

        # Create a sample DataFrame to upload.
        data = {'product_id': [101, 102], 'product_name': ['Gadget', 'Widget']}
        df_to_push = pd.DataFrame(data)

        connector = BQConnector()
        try:
            connector.connect()
            # Define the destination table.
            table_name = 'test_table'

            # The worker needs an active connector.
            worker = PushWorker(connector)
            worker.push(
                df=df_to_push,
                project_id=connector.project_id,
                dataset=connector.dataset,
                table=table_name,
                write_disposition='WRITE_TRUNCATE',
            )
            print(f'Successfully pushed data to {table_name}')
        finally:
            connector.close()
        ```
    """

    def __init__(self, connector: BQConnector):
        """
        Initializes the PushWorker.

        Args:
            connector: An initialized and connected `BQConnector`
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
        Loads a pandas DataFrame into a BigQuery table.

        This method handles the entire process of uploading a DataFrame,
        including job configuration, execution, and error checking.

        Args:
            df: The pandas DataFrame to be uploaded.
            project_id: The GCP project ID. If None, the project ID from
                the active connector is used.
            dataset: The BigQuery dataset ID. If None, the dataset from
                the active connector is used.
            table: The destination table ID. If None, the table from the
                active connector is used.
            schema: An optional list of 'bigquery.SchemaField' objects.
                If None, BigQuery attempts schema auto-detection.
            write_disposition: Specifies the action if the table exists
                (e.g., 'WRITE_APPEND', 'WRITE_TRUNCATE'). Defaults to
                'WRITE_APPEND'.

        Raises:
            RuntimeError: If the BigQuery client is not initialized or if
                the load job fails after execution.
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
