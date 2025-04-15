from clickhouse_client import ClickHouseClient
from flatfile_client import FlatFileClient


class IngestionService:
    def run_ingestion(self, source_type, target_type, ch_host, ch_port,
                      ch_db, ch_user, jwt_token, ch_table, flatfile_name, columns):

        if source_type == "ClickHouse" and target_type == "FlatFile":
            ch_client = ClickHouseClient(ch_host, ch_port, ch_db, ch_user, jwt_token)
            data = ch_client.fetch_data(ch_table, columns)
            flatfile = FlatFileClient(flatfile_name)
            flatfile.write_data(flatfile_name, columns, data)
            return len(data)

        elif source_type == "FlatFile" and target_type == "ClickHouse":
            flatfile = FlatFileClient(flatfile_name)
            data = flatfile.read_data(columns)

            ch_client = ClickHouseClient(ch_host, ch_port, ch_db, ch_user, jwt_token)
            ch_client.insert_data(ch_table, columns, data)
            return len(data)

        else:
            raise Exception("Invalid source/target combination")

