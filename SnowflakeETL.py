import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import configparser
import requests

# This function retrieves data from a relational database
def getFromDatabase(query):
    # Read credentials from the config file
    config = configparser.ConfigParser()
    config.read('credentials.ini')
    
    dbCredentials = config['DATABASE']
    
    # Create a connection to the database using SQLAlchemy
    engine = create_engine(f"mssql+pyodbc://{dbCredentials['username']}:{dbCredentials['password']}@{dbCredentials['host']}/{dbCredentials['dbname']}?driver=ODBC+Driver+17+for+SQL+Server")
    # Execute the query and load the results into a Pandas DataFrame
    dbData = pd.read_sql(query, engine)
    return dbData

# This function retrieves data from API
def getFromApi():
    # Read API credentials from config file
    config = configparser.ConfigParser()
    config.read('credentials.ini')
    apiCredentials = config['API']
    
    # Define the API endpoint URL
    apiUrl = apiCredentials['url']
    # Make a GET request to the API to retrieve data
    response = requests.get(apiUrl, headers={"Authorization": f"Bearer {apiCredentials['token']}"})
    # Convert the JSON response into a Pandas DataFrame
    apiData = pd.DataFrame(response.json())
    return apiData

# This function retrieves data from CSV file
def getFromCsv(filePath):
    csvData = pd.read_csv(filePath)
    return csvData

def setTransformedData(dbData, apiData, csvData):
    # Fill missing values in the data
    dbData = dbData.fillna('default_value')
    apiData = apiData.fillna('default_value')
    csvData = csvData.fillna('default_value')

    # Remove duplicate rows from each dataset
    dbData = dbData.drop_duplicates()
    apiData = apiData.drop_duplicates()
    csvData = csvData.drop_duplicates()

    # Merge all three datasets on a common column
    combinedData = pd.merge(dbData, apiData, on='common_column', how='inner')
    combinedData = pd.merge(combinedData, csvData, on='common_column', how='inner')

    return combinedData

def setToSnowflake():
    """Establish connection to Snowflake."""
    # Read Snowflake credentials from config file
    config = configparser.ConfigParser()
    config.read('credentials.ini')
    
    snowflakeCredentials = config['SNOWFLAKE']
    
    # Establish a connection to Snowflake using credentials
    conn = snowflake.connector.connect(
        user=snowflakeCredentials['user'],
        password=snowflakeCredentials['password'],
        account=snowflakeCredentials['account'],
        warehouse=snowflakeCredentials['warehouse'],
        database=snowflakeCredentials['database'],
        schema=snowflakeCredentials['schema']
    )
    return conn

def main():
  
    print("Starting pipeline...")
    print("Extracting from database...")
    query = "SELECT * FROM table1"
    dbData = getFromDatabase(query)
    
    print("Extracting from API...")
    apiData = getFromApi()

    print("Extracting from CSV...")
    csvData = getFromCsv("data.csv")
    
    print("Transforming data...")
    transformedData = setTransformedData(dbData, apiData, csvData)

    print("Connecting to Snowflake...")
    conn = setToSnowflake()
    
    print("Loading data to Snowflake...")
    try:
        write_pandas(conn, transformedData, 'table_name', auto_create_table=True)
        print("Data successfully loaded into Snowflake.")
    except Exception as e:
        print(f"An error occurred while inserting data into Snowflake: {e}")
    finally:
        conn.close()

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
