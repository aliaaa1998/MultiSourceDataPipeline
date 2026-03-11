# MultiSourceDataPipeline

A Python ETL pipeline that extracts data from:
- a SQL Server database,
- a REST API,
- and a CSV file,

then transforms and loads the merged dataset into Snowflake.

## What was improved

- Added structured logging and clearer pipeline stages.
- Added centralized configuration loading with validation.
- Added safer API handling (`timeout`, `raise_for_status`).
- Added merge-key validation for all sources before transformation.
- Added better resource cleanup (`engine.dispose()` and Snowflake connection close).
- Made key runtime options configurable through a new optional `[PIPELINE]` section.

## Project files

- `SnowflakeETL.py`: main ETL script.
- `credentials.ini` (you create this): credentials and pipeline options.

## Requirements

- Python 3.8+
- SQL Server ODBC driver (`ODBC Driver 17 for SQL Server`)

Install Python packages:

```bash
pip install pandas sqlalchemy pyodbc snowflake-connector-python requests
```

## Configuration

Create `credentials.ini` in the project root:

```ini
[DATABASE]
username = your_db_username
password = your_db_password
host = your_db_host
dbname = your_db_name

[API]
url = your_api_url
token = your_api_token

[SNOWFLAKE]
user = your_snowflake_user
password = your_snowflake_password
account = your_snowflake_account
warehouse = your_snowflake_warehouse
database = your_snowflake_database
schema = your_snowflake_schema

[PIPELINE]
# Optional values (shown with defaults)
source_csv_path = data.csv
merge_key = common_column
target_table = table_name
source_query = SELECT * FROM table1
```

## Run

```bash
python SnowflakeETL.py
```

## Data flow

1. **Extract**
   - DB query from `[PIPELINE].source_query`
   - API call using bearer token
   - CSV read from `[PIPELINE].source_csv_path`
2. **Transform**
   - Fill missing values with `default_value`
   - Remove duplicate rows per source
   - Inner-join all three sources on `[PIPELINE].merge_key`
3. **Load**
   - Write merged dataframe to Snowflake table `[PIPELINE].target_table`
   - Auto-create the Snowflake table if needed

## Troubleshooting

- **`Configuration file not found`**
  - Ensure `credentials.ini` exists in the repository root.
- **`Missing required config section(s)`**
  - Confirm `[DATABASE]`, `[API]`, and `[SNOWFLAKE]` sections are present.
- **Merge key errors**
  - Ensure the configured `merge_key` exists in all three datasets.
- **SQL Server connection issues**
  - Verify DB host/name/user/password and ensure ODBC Driver 17 is installed.
- **API failures**
  - Check API URL/token and verify endpoint availability.
- **Snowflake load failures**
  - Confirm Snowflake credentials, permissions, and target warehouse/database/schema.
