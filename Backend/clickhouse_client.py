import clickhouse_connect
from fastapi import HTTPException


class ClickHouseClient:
    def __init__(self, host: str, port: int, database: str, user: str, jwt_token: str):
        try:
            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=jwt_token,
                database=database,
                secure=True
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Connection Error: {str(e)}")

    def get_tables(self):
        try:
            result = self.client.query("SHOW TABLES")
            return [row[0] for row in result.result_rows]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error fetching tables: {str(e)}")

    def get_columns(self, table_name: str):
        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.client.query(query)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error describing table: {str(e)}")

    def fetch_data(self, table_name: str, columns: list):
        try:
            column_str = ", ".join(columns)
            query = f"SELECT {column_str} FROM {table_name}"
            result = self.client.query(query)
            return result.result_rows
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error fetching data: {str(e)}")

    def insert_data(self, table_name: str, columns: list, values: list):
        try:
            self.client.insert(table=table_name, column_names=columns, data=values)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error inserting data: {str(e)}")

