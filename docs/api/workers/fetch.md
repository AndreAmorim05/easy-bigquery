# FetchWorker

The `FetchWorker` class handles data retrieval from BigQuery into pandas DataFrames.

## Overview

`FetchWorker` encapsulates the logic for executing SQL queries against BigQuery and loading results into pandas DataFrames. It requires an active `BQConnector` instance and provides efficient data fetching using the BigQuery Storage API.

## Class Reference

::: easy_bigquery.workers.fetch.FetchWorker
    handler: python
    selection:
      members:
        - __init__
        - fetch
    rendering:
      show_source: true
      show_root_heading: true

## Usage Examples

### Basic Data Fetching

```python
from easy_bigquery import BQConnector
from easy_bigquery.workers import FetchWorker

# Create and connect connector
connector = BQConnector()
connector.connect()

try:
    # Create fetch worker
    worker = FetchWorker(connector)
    
    # Execute query
    sql = "SELECT name, state FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 5"
    df = worker.fetch(sql)
    
    print("Fetched data:")
    print(df)
    
finally:
    connector.close()
```

### Using BigQuery Storage API

```python
from easy_bigquery import BQConnector
from easy_bigquery.workers import FetchWorker

connector = BQConnector()
connector.connect()

try:
    worker = FetchWorker(connector)
    
    # Use BigQuery Storage API for faster downloads (default)
    df = worker.fetch(
        "SELECT * FROM my_large_table",
        use_storage_api=True
    )
    
    # Disable Storage API for compatibility
    df = worker.fetch(
        "SELECT * FROM my_table",
        use_storage_api=False
    )
    
finally:
    connector.close()
```

### Custom Query Parameters

```python
from easy_bigquery import BQConnector
from easy_bigquery.workers import FetchWorker

connector = BQConnector()
connector.connect()

try:
    worker = FetchWorker(connector)
    
    # Pass additional parameters to to_dataframe()
    df = worker.fetch(
        "SELECT * FROM my_table",
        dtypes={'id': 'int64', 'name': 'string'},
        date_as_object=False
    )
    
finally:
    connector.close()
```

## Parameters

### fetch() Method

- _query_ (str): The SQL query string to execute
- _use_storage_api_ (bool, optional): Whether to use BigQuery Storage API for faster downloads. Defaults to `True`.
- _\*\*kwargs_: Additional keyword arguments passed to the underlying `to_dataframe()` method

## Performance Considerations

### BigQuery Storage API

The BigQuery Storage API provides significantly faster data downloads compared to the standard BigQuery API:

- _Enabled by default_ (`use_storage_api=True`)
- _Recommended_ for large datasets
- _May not be available_ in all environments (use `use_storage_api=False` if needed)

### Query Optimization

For best performance:

1. _Limit results_ when possible: `SELECT * FROM table LIMIT 1000`
2. _Use specific columns_ instead of `SELECT *`
3. _Add WHERE clauses_ to filter data at the source
4. _Use partitioning_ for large tables

## Error Handling

The worker will raise appropriate exceptions for common issues:

```python
try:
    df = worker.fetch("SELECT * FROM non_existent_table")
except Exception as e:
    print(f"Query failed: {e}")
```

Common error scenarios:

- _Table not found_: Invalid table name or insufficient permissions
- _Syntax errors_: Malformed SQL queries
- _Authentication errors_: Invalid or expired credentials
- _Network errors_: Connectivity issues

## Best Practices

1. _Always close connections_ after use
2. _Use appropriate query limits_ for large datasets
3. _Handle exceptions_ gracefully
4. _Use BigQuery Storage API_ when possible
5. _Optimize queries_ for performance
6. _Validate query results_ before processing

## Integration with BQManager

For most use cases, it's recommended to use `BQManager` instead of `FetchWorker` directly:

```python
from easy_bigquery import BQManager

# Simpler and safer approach
with BQManager() as bq:
    df = bq.fetch("SELECT * FROM my_table")
    # Connection automatically closed
```
