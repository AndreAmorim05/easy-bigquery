# BQConnector

The `BQConnector` class is the core component responsible for managing connections to Google BigQuery.

## Overview

`BQConnector` handles the low-level connection logic to Google BigQuery, including authentication, client instantiation, and connection lifecycle management.

## Class Reference

::: easy_bigquery.connector.connector.BQConnector
    handler: python
    selection:
      members:
        - __init__
        - connect
        - close
    rendering:
      show_source: true
      show_root_heading: true

## Usage Examples

### Basic Connection

```python
from easy_bigquery import BQConnector

# Create connector instance
connector = BQConnector(
    project_id="your-project-id",
    credentials_info='{"type": "service_account", ...}',
    dataset="your_dataset",
    table="your_table"
)

try:
    # Establish connection
    connector.connect()
    
    # Use the connector for operations
    print(f"Connected to project: {connector.project_id}")
    
finally:
    # Always close the connection
    connector.close()
```

### Using Environment Variables

```python
from easy_bigquery import BQConnector

# Uses environment variables by default
connector = BQConnector()

try:
    connector.connect()
    # Your BigQuery operations here
finally:
    connector.close()
```

## Configuration

The connector can be configured using the following parameters:

- _project_id_: Your Google Cloud Project ID
- _credentials_info_: JSON string containing service account credentials
- _dataset_: Default BigQuery dataset name
- _table_: Default BigQuery table name

## Error Handling

The connector will raise appropriate exceptions for common issues:

- _Authentication errors_: Invalid or missing credentials
- _Connection errors_: Network or API access issues
- _Configuration errors_: Missing required parameters

## Best Practices

1. _Always use context managers_ when possible (see `BQManager`)
2. _Close connections_ explicitly when done
3. _Handle exceptions_ appropriately
4. _Store credentials securely_ using environment variables
5. _Use least privilege_ service accounts
