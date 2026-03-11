import configparser
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DEFAULT_CONFIG_PATH = Path("credentials.ini")
DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"


@dataclass
class PipelineConfig:
    """Container for all pipeline configuration values."""

    db_username: str
    db_password: str
    db_host: str
    db_name: str
    api_url: str
    api_token: str
    snowflake_user: str
    snowflake_password: str
    snowflake_account: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    source_csv_path: str = "data.csv"
    merge_key: str = "common_column"
    target_table: str = "table_name"
    source_query: str = "SELECT * FROM table1"


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> PipelineConfig:
    """Read credentials and runtime options from an INI file."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}. "
            "Create credentials.ini before running the pipeline."
        )

    parser = configparser.ConfigParser()
    parser.read(config_path)

    required_sections = {"DATABASE", "API", "SNOWFLAKE"}
    missing_sections = required_sections.difference(parser.sections())
    if missing_sections:
        missing = ", ".join(sorted(missing_sections))
        raise KeyError(f"Missing required config section(s): {missing}")

    return PipelineConfig(
        db_username=parser["DATABASE"]["username"],
        db_password=parser["DATABASE"]["password"],
        db_host=parser["DATABASE"]["host"],
        db_name=parser["DATABASE"]["dbname"],
        api_url=parser["API"]["url"],
        api_token=parser["API"]["token"],
        snowflake_user=parser["SNOWFLAKE"]["user"],
        snowflake_password=parser["SNOWFLAKE"]["password"],
        snowflake_account=parser["SNOWFLAKE"]["account"],
        snowflake_warehouse=parser["SNOWFLAKE"]["warehouse"],
        snowflake_database=parser["SNOWFLAKE"]["database"],
        snowflake_schema=parser["SNOWFLAKE"]["schema"],
        source_csv_path=parser.get("PIPELINE", "source_csv_path", fallback="data.csv"),
        merge_key=parser.get("PIPELINE", "merge_key", fallback="common_column"),
        target_table=parser.get("PIPELINE", "target_table", fallback="table_name"),
        source_query=parser.get("PIPELINE", "source_query", fallback="SELECT * FROM table1"),
    )


def create_db_engine(config: PipelineConfig) -> Engine:
    connection_url = (
        "mssql+pyodbc://"
        f"{config.db_username}:{config.db_password}@{config.db_host}/{config.db_name}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(connection_url)


def get_from_database(engine: Engine, query: str) -> pd.DataFrame:
    logging.info("Extracting data from database")
    return pd.read_sql(query, engine)


def get_from_api(url: str, token: str, timeout_seconds: int = 30) -> pd.DataFrame:
    logging.info("Extracting data from API")
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    api_payload = response.json()
    if isinstance(api_payload, dict):
        # Normalize dictionary responses into a one-row table.
        return pd.json_normalize(api_payload)
    return pd.DataFrame(api_payload)


def get_from_csv(file_path: str) -> pd.DataFrame:
    logging.info("Extracting data from CSV: %s", file_path)
    return pd.read_csv(file_path)


def validate_merge_key(dataframe: pd.DataFrame, merge_key: str, source_name: str) -> None:
    if merge_key not in dataframe.columns:
        raise KeyError(f"Merge key '{merge_key}' not found in {source_name} dataset")


def transform_data(
    db_data: pd.DataFrame,
    api_data: pd.DataFrame,
    csv_data: pd.DataFrame,
    merge_key: str,
) -> pd.DataFrame:
    logging.info("Transforming data")

    validate_merge_key(db_data, merge_key, "database")
    validate_merge_key(api_data, merge_key, "api")
    validate_merge_key(csv_data, merge_key, "csv")

    db_data = db_data.fillna("default_value").drop_duplicates()
    api_data = api_data.fillna("default_value").drop_duplicates()
    csv_data = csv_data.fillna("default_value").drop_duplicates()

    combined_data = pd.merge(db_data, api_data, on=merge_key, how="inner")
    combined_data = pd.merge(combined_data, csv_data, on=merge_key, how="inner")
    logging.info("Transformation complete. Rows after merge: %s", len(combined_data))
    return combined_data


def create_snowflake_connection(config: PipelineConfig) -> SnowflakeConnection:
    logging.info("Connecting to Snowflake")
    return snowflake.connector.connect(
        user=config.snowflake_user,
        password=config.snowflake_password,
        account=config.snowflake_account,
        warehouse=config.snowflake_warehouse,
        database=config.snowflake_database,
        schema=config.snowflake_schema,
    )


def load_to_snowflake(conn: SnowflakeConnection, transformed_data: pd.DataFrame, table_name: str) -> None:
    logging.info("Loading %s row(s) into Snowflake table '%s'", len(transformed_data), table_name)
    success, nchunks, nrows, _ = write_pandas(
        conn,
        transformed_data,
        table_name,
        auto_create_table=True,
    )
    if not success:
        raise RuntimeError("write_pandas reported a failure while loading data.")
    logging.info("Snowflake load complete. Chunks: %s, Rows: %s", nchunks, nrows)


def main(config_path: Optional[Path] = None) -> None:
    logging.basicConfig(level=logging.INFO, format=DEFAULT_LOG_FORMAT)
    config = load_config(config_path or DEFAULT_CONFIG_PATH)

    engine = create_db_engine(config)
    conn: Optional[SnowflakeConnection] = None

    try:
        db_data = get_from_database(engine, config.source_query)
        api_data = get_from_api(config.api_url, config.api_token)
        csv_data = get_from_csv(config.source_csv_path)

        transformed_data = transform_data(db_data, api_data, csv_data, config.merge_key)

        conn = create_snowflake_connection(config)
        load_to_snowflake(conn, transformed_data, config.target_table)
        logging.info("Pipeline completed successfully")
    finally:
        engine.dispose()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
