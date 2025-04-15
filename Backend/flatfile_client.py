import pandas as pd
import os

class FlatFileClient:
    def __init__(self, file_name: str, delimiter=","):
        self.file_path = f"../data/uploads/{file_name}"
        self.delimiter = delimiter

    def get_columns(self):
        df = pd.read_csv(self.file_path, delimiter=self.delimiter, nrows=1)
        return list(df.columns)

    def read_data(self, selected_columns):
        df = pd.read_csv(self.file_path, delimiter=self.delimiter, usecols=selected_columns)
        return df.values.tolist()

    def write_data(self, file_name, columns, rows):
        df = pd.DataFrame(rows, columns=columns)
        output_path = f"../data/output/{file_name}"
        df.to_csv(output_path, index=False)
        return output_path

