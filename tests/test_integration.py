import os
import pytest
from backend.ingestion_service import ingest_flatfile_to_clickhouse
from backend.config import CLICKHOUSE_CONFIG
from clickhouse_driver import Client

@pytest.fixture
def sample_file():
    return os.path.join("test_data", "sample1.csv")

def test_flatfile_to_clickhouse_ingestion(sample_file):
    table_name = "test_people"
    result = ingest_flatfile_to_clickhouse(sample_file, table_name, delimiter=",")
    assert result["status"] == "success"
    assert result["records_ingested"] > 0

    # Check in ClickHouse if table exists
    client = Client(
        host=CLICKHOUSE_CONFIG["host"],
        port=CLICKHOUSE_CONFIG["port"],
        user=CLICKHOUSE_CONFIG["user"],
        password=CLICKHOUSE_CONFIG["password"],
        database=CLICKHOUSE_CONFIG["database"],
        secure=True
    )
    res = client.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert res[0][0] == result["records_ingested"]
