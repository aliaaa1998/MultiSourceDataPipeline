# MultiSourceDataPipeline
# Data Pipeline README

## Overview
This data pipeline extracts data from three sources: a relational database, a REST API, and a CSV file. The data is cleaned, transformed, and then loaded into a Snowflake database.

## Setup

### Prerequisites
- Python 3.8 or higher
- Required Python libraries:
  - `pandas`
  - `sqlalchemy`
  - `snowflake-connector-python`
  - `snowflake-connector-pandas`
  - `configparser`
  - `requests`
- Access to the following:
  - Relational database (e.g., SQL Server)
  - REST API endpoint
  - Snowflake account

### Installation
1. Clone this repository or download the code file.
2. Install the required Python libraries using:
   ```bash
   pip install pandas sqlalchemy snowflake-connector-python snowflake-connector-pandas requests
   ```

### Configuration
1. Create a configuration file named `credentials.ini` in the root directory with the following structure:

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
   ```

2. Ensure that the CSV file is placed in the same directory as the code or specify its path in the `getFromCsv` function.

## Assumptions
1. The common column name between the three data sources is `common_column`.
2. The Snowflake table will be automatically created if it does not exist.
3. Default values are used to fill missing data in the pipeline.
4. Duplicate rows are removed from each data source before merging.

## How to Run the Pipeline
1. Ensure all required dependencies are installed and the `credentials.ini` file is correctly configured.
2. Run the Python script:
   ```bash
   python data_pipeline_sample.py
   ```
3. The pipeline will:
   - Extract data from the relational database using a specified SQL query.
   - Extract data from the REST API using the provided endpoint and token.
   - Extract data from the CSV file.
   - Transform the data by cleaning, deduplicating, and merging it.
   - Load the transformed data into Snowflake.
4. Check the console output for logs indicating the progress and success/failure of each step.

## Notes
- Make sure to update the SQL query in the `main` function to match your requirements.
- Ensure Snowflake credentials have the appropriate permissions to create tables and insert data.
- Modify the column names and data structure as needed to align with your data schema.

## Troubleshooting
- **Connection Issues**: Verify that your credentials in `credentials.ini` are correct.
- **Missing Libraries**: Reinstall dependencies using the `pip install` command.
- **Data Merging Errors**: Ensure the `common_column` exists and matches across all data sources.

## License
This project is licensed under the MIT License. Feel free to use and modify as needed.

