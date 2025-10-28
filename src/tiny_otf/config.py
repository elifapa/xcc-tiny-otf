from enum import Enum
from tiny_otf.storage.storage import LocalFSDataStorage, MinioDataStorage


STORAGE_TYPE = "minio"
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

class Plans(Enum):
    """
    Enum for different query plans.
    """
    CREATE = "CreateTablePlan"
    INSERT = "InsertPlan"
    SELECT = "SelectPlan"

class StorageConfig(Enum):
    LOCAL_FS = LocalFSDataStorage
    MINIO = MinioDataStorage

def initialize_storage(storage_type: str = STORAGE_TYPE, **kwargs):
    """
    Initialize the storage layer based on the storage configuration.
    """
    try:
        storage_config = StorageConfig[storage_type.upper()]
        print("Initializing storage with type:", storage_type)
        return storage_config.value(base_path=STORAGE_PATH, **kwargs)
    except KeyError:
        raise ValueError(f"Unsupported storage type: {STORAGE_TYPE}")