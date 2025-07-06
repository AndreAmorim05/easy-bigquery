# PushWorker

The `PushWorker` class handles uploading pandas DataFrames to BigQuery tables.

## Overview

`PushWorker` encapsulates the logic for loading pandas DataFrames into BigQuery tables. It requires an active `BQConnector` instance and provides flexible options for data upload including different write dispositions and schema handling.

## Class Reference

::: easy_bigquery.workers.push.PushWorker
    handler: python
    selection:
      members:
        - __init__
        - push
    rendering:
      show_source: true
      show_root_heading: true

## Usage Examples

### Basic Data Upload

```python
import pandas as pd
from easy_bigquery import BQConnector
from easy_bigquery.workers import PushWorker

# Create sample data
data = {
    'user_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'status': ['active', 'inactive', 'active', 'active', 'inactive']
}
df = pd.DataFrame(data)

# Create and connect connector
connector = BQConnector()
connector.connect()

try:
    # Create push worker
    worker = PushWorker(connector)
    
    # Upload data
    worker.push(
        df=df,
        table='users',
        write_disposition='WRITE_APPEND'
    )
    
    print("Data uploaded successfully!")
    
finally:
    connector.close()
```

### Different Write Dispositions

```python
import pandas as pd
from easy_bigquery import BQConnector
from easy_bigquery.workers import PushWorker

connector = BQConnector()
connector.connect()

try:
    worker = PushWorker(connector)
    
    # Append data to existing table
    worker.push(
        df=df,
        table='users',
        write_disposition='WRITE_APPEND'
    )
    
    # Replace all data in table
    worker.push(
        df=df,
        table='users',
        write_disposition='WRITE_TRUNCATE'
    )
    
    # Only write if table is empty
    worker.push(
        df=df,
        table='users',
        write_disposition='WRITE_EMPTY'
    )
    
finally:
    connector.close()
```

### Custom Schema

```python
from google.cloud import bigquery as bq
from easy_bigquery import BQConnector
from easy_bigquery.workers import PushWorker

connector = BQConnector()
connector.connect()

try:
    worker = PushWorker(connector)
    
    # Define custom schema
    schema = [
        bq.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bq.SchemaField("name", "STRING", mode="REQUIRED"),
        bq.SchemaField("email", "STRING", mode="NULLABLE"),
        bq.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
    ]
    
    # Upload with custom schema
    worker.push(
        df=df,
        table='users',
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )
    
finally:
    connector.close()
```

### Using Different Projects and Datasets

```python
from easy_bigquery import BQConnector
from easy_bigquery.workers import PushWorker

connector = BQConnector()
connector.connect()

try:
    worker = PushWorker(connector)
    
    # Upload to different project/dataset
    worker.push(
        df=df,
        project_id='my-other-project',
        dataset='analytics',
        table='user_events',
        write_disposition='WRITE_APPEND'
    )
    
finally:
    connector.close()
```

## Parameters

### push() Method

- _df_ (pandas.DataFrame): The DataFrame to upload
- _project_id_ (str, optional): GCP project ID. If None, uses connector's project_id
- _dataset_ (str, optional): BigQuery dataset ID. If None, uses connector's dataset
- _table_ (str, optional): Destination table ID. If None, uses connector's table
- _schema_ (List[SchemaField], optional): Custom schema definition
- _write_disposition_ (str): Write mode for the upload operation

## Write Dispositions

| Disposition | Description |
|-------------|-------------|
| `WRITE_APPEND` | Add data to existing table (default) |
| `WRITE_TRUNCATE` | Replace all data in table |
| `WRITE_EMPTY` | Only write if table is empty |
| `WRITE_DISPOSITION_UNSPECIFIED` | Use BigQuery default behavior |

## Schema Handling

### Auto-detection (Default)

When no schema is provided, BigQuery automatically detects the schema from the DataFrame:

```python
# BigQuery will infer schema from DataFrame columns and types
worker.push(df=df, table='users')
```

For precise control over data types and constraints:

```python
from google.cloud import bigquery as bq

schema = [
    bq.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bq.SchemaField("name", "STRING", mode="REQUIRED"),
    bq.SchemaField("email", "STRING", mode="NULLABLE"),
    bq.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
]

worker.push(df=df, table='users', schema=schema)
```

## Error Handling

The worker provides detailed error information for upload failures:

```python
try:
    worker.push(df=df, table='users')
except Exception as e:
    print(f"Upload failed: {e}")
    # Check for specific error types and handle accordingly
```

Common error scenarios:

- _Permission denied_: Insufficient BigQuery permissions
- _Table not found_: Invalid table name or dataset
- _Schema mismatch_: DataFrame structure doesn't match table schema
- _Data type errors_: Incompatible data types
- _Quota exceeded_: BigQuery quota limits reached

## Performance Considerations

### Large Datasets

For large DataFrames:

1. _Chunk data_ into smaller pieces
2. _Use appropriate write dispositions_
3. _Monitor BigQuery quotas_
4. _Consider streaming inserts_ for real-time data

### Best Practices

```python
# For large datasets, consider chunking
chunk_size = 10000
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    worker.push(
        df=chunk,
        table='large_table',
        write_disposition='WRITE_APPEND'
    )
```

## Integration with BQManager

For most use cases, it's recommended to use `BQManager` instead of `PushWorker` directly:

```python
from easy_bigquery import BQManager

# Simpler and safer approach
with BQManager() as bq:
    bq.push(df=df, table='users')
    # Connection automatically closed
```

## Data Type Considerations

BigQuery has specific data type requirements:

- _INTEGER_: Use for whole numbers
- _FLOAT_: Use for decimal numbers
- _STRING_: Use for text data
- _TIMESTAMP_: Use for datetime data
- _BOOLEAN_: Use for true/false values

Ensure your DataFrame data types are compatible with BigQuery expectations.
