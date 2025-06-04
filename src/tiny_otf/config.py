STORAGE_PATH = "data/"
METADATA_PATH = "src/tiny_otf/table_catalog/table_metadata.json"

SQL_TO_PANDAS_TYPES = {
    "INT": "int64",
    "INTEGER": "int64",
    "BIGINT": "int64",
    "FLOAT": "float64",
    "DOUBLE": "float64",
    "VARCHAR": "string",
    "TEXT": "string",
    "DATE": "datetime64[ns]",
    "BOOLEAN": "bool"
}
