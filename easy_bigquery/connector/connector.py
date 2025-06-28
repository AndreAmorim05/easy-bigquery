import json
from typing import Any, Dict, Optional

from google.cloud import bigquery as bq
from google.cloud.bigquery_storage import BigQueryReadClient
from google.oauth2 import service_account

from easy_bigquery.core.config import (
    BQ_DATASET,
    BQ_JSON_CREDENTIALS,
    BQ_PROJECT_ID,
    BQ_TABLE_NAME,
)
from easy_bigquery.logger.manager import logger


class BQConnector:
    """
    Manages the low-level connection to Google BigQuery.

    This class is the core of the connection logic. It is responsible for
    authenticating, establishing client sessions with both the standard
    BigQuery API and the BigQuery Storage API, and properly closing
    them. It is designed to be instantiated and managed by a
    higher-level class (like a facade) or used directly when manual
    control over the connection lifecycle is required.

    Attributes:
        project_id (str): The GCP project ID for the connection.
        dataset (str): The default dataset to be used.
        credentials (Optional[service_account.Credentials]): The
            authenticated gcloud credentials object after connection.
        client (Optional[bq.Client]): The main BigQuery client.
        bq_storage (Optional[BigQueryReadClient]): The BigQuery
            Storage API client, used for fast data downloads.

    Example:
        ```python
        # Manual Connection Management
        from easy_bigquery import BQConnector

        # The connector is instantiated but not yet connected.
        connector = BQConnector()

        try:
            # Manually establish the connection.
            connector.connect()
            print(f'Client is active: {connector.client is not None}')

            # Now you can pass this 'connector' instance to a
            # Fetcher or Pusher class for operations.

        finally:
            # Always ensure the connection is closed.
            connector.close()
        ```
    """

    def __init__(
        self,
        project_id: str = BQ_PROJECT_ID,
        credentials_info: str = BQ_JSON_CREDENTIALS,
        dataset: str = BQ_DATASET,
        table: str = BQ_TABLE_NAME,
    ):
        """
        Initializes the BQConnector.

        Args:
            project_id: The GCP project ID. Defaults to the value from
                the environment configuration.
            credentials_info: A JSON string of the service account
                credentials. Defaults to the value from the
                environment configuration.
            dataset: The default BigQuery dataset name. Defaults to the
                value from the environment configuration.
            table: The default BigQuery table name. Defaults to the
                value from the environment configuration.
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self._creds_info: Dict[str, Any] = json.loads(credentials_info)
        self.credentials: Optional[service_account.Credentials] = None
        self.client: Optional[bq.Client] = None
        self.bq_storage: Optional[BigQueryReadClient] = None

    def connect(self) -> None:
        """Establishes connections to BigQuery clients."""
        logger.info(f'Connecting to BigQuery project: {self.project_id}')
        self.credentials = (
            service_account.Credentials.from_service_account_info(
                info=self._creds_info
            )
        )
        self.client = bq.Client(
            credentials=self.credentials, project=self.project_id
        )
        self.bq_storage = BigQueryReadClient(credentials=self.credentials)
        logger.info('BigQuery clients created successfully.')

    def close(self) -> None:
        """Closes all active BigQuery connections."""
        if self.bq_storage and hasattr(self.bq_storage.transport, 'close'):
            self.bq_storage.transport.close()
        self.client = None
        self.bq_storage = None
        logger.info('BigQuery connections closed.')
