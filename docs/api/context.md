# BQManager

The `BQManager` class provides a high-level context manager interface for BigQuery operations.

## Overview

`BQManager` is the recommended entry point for most users. It simplifies BigQuery interactions by providing an easy-to-use context manager that handles connection lifecycle automatically.

## Class Reference

::: easy_bigquery.context.manager.BQManager
    handler: python
    selection:
      members:
        - __init__
        - __enter__
        - __exit__
        - fetch
        - push
    rendering:
      show_source: true
      show_root_heading: true

## Usage Examples

### Basic Usage with Context Manager

```python
import pandas as pd
from easy_bigquery import BQManager

# Using the context manager (recommended)
with BQManager() as bq:
    # Fetch data from a public dataset
    sql = "SELECT * FROM `bigquery-public-data.usa_names.usa_1910_current` LIMIT 10"
    df = bq.fetch(sql)
    print(df.head())
    
    # Push data to your dataset
    new_data = pd.DataFrame({
        'user_id': [1, 2, 3],
        'status': ['active', 'inactive', 'active']
    })
    
    bq.push(
        df=new_data,
        table='users',
        write_disposition='WRITE_APPEND'
    )
```

### Custom Configuration

```python
from easy_bigquery import BQManager

# Pass custom configuration
with BQManager(
    project_id="my-custom-project",
    dataset="my_dataset"
) as bq:
    # Operations with custom config
    data = bq.fetch("SELECT * FROM my_table LIMIT 5")
```

### Manual Connection Management

```python
from easy_bigquery import BQManager

# Manual control (not recommended for most cases)
manager = BQManager()

try:
    # Enter context manually
    manager.__enter__()
    
    # Your operations
    data = manager.fetch("SELECT * FROM table")
    
finally:
    # Exit context manually
    manager.__exit__(None, None, None)
```

## Methods

### fetch()

Executes a SQL query and returns results as a pandas DataFrame.

```python
# Basic fetch
df = bq.fetch("SELECT * FROM my_table LIMIT 10")

# With custom parameters
df = bq.fetch(
    "SELECT * FROM my_table WHERE status = 'active'",
    use_storage_api=True  # Use faster BigQuery Storage API
)
```

### push()

Uploads a pandas DataFrame to a BigQuery table.

```python
import pandas as pd

# Prepare data
data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})

# Push to BigQuery
bq.push(
    df=data,
    table='users',
    write_disposition='WRITE_APPEND'  # Append to existing table
)
```

## Write Dispositions

The `push()` method supports different write dispositions:

- `WRITE_APPEND`: Add data to existing table (default)
- `WRITE_TRUNCATE`: Replace all data in table
- `WRITE_EMPTY`: Only write if table is empty
- `WRITE_DISPOSITION_UNSPECIFIED`: Use BigQuery default

## Error Handling

The context manager automatically handles connection cleanup, but you should still handle application-specific errors:

```python
try:
    with BQManager() as bq:
        data = bq.fetch("SELECT * FROM non_existent_table")
except Exception as e:
    print(f"Error: {e}")
    # Connection is automatically closed
```

## Best Practices

1. _Always use the context manager_ (`with` statement)
2. _Handle exceptions_ appropriately
3. _Use appropriate write dispositions_ for your use case
4. _Validate data_ before pushing to BigQuery
5. _Use BigQuery Storage API_ for large datasets (enabled by default)
