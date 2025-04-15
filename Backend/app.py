from pydantic import BaseModel

class ClickHouseConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    jwt_token: str

class IngestionRequest(BaseModel):
    source_type: str
    target_type: str
    clickhouse_config: ClickHouseConfig
    flatfile_name: str
    columns: list

