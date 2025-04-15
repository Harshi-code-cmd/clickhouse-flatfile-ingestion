def convert_columns_string(columns_str: str):
    return [col.strip() for col in columns_str.split(',')]
