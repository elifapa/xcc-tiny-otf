import json
from pathlib import Path

from tiny_otf.storage.storage import BaseStorage, ParquetDataStorage
from tiny_otf.config import METADATA_PATH

class TableMetadata:
    def __init__(self):
        self.catalog_path = Path(METADATA_PATH)
        self._catalog = {}
        self._load()

    # Helper functions to read from and write to json files 
    def _load(self):

        if self.catalog_path.exists():
            print("Metadata source exists, nice!")
            with open(self.catalog_path, "r") as f:
                self._catalog = json.load(f)
        # Initialise the json file as empty
        else:
            print("Metadata does not exist, initialising an empty one...")
            self._catalog = {}

    def _save(self):
        with open(self.catalog_path, "w") as f:
            json.dump(self._catalog, f, indent=2)

    def get_table(self, name: str) -> dict:
        return self._catalog.get(name)

    def list_tables(self) -> list:
        return list(self._catalog.keys())

    def add_table(self, name: str, columns:list[dict[str, str]]) -> None:
        if name in self._catalog:
            raise ValueError(f"Table '{name}' already exists.")
        
        metadata = {
            "schema": columns,
            "storage": {
                "format": "parquet",
                "path": f"data/{name}"
            }
        }
        self._catalog[name] = metadata
        self._save()

    def update_table(self, name: str, metadata: dict) -> None:
        if name not in self._catalog:
            raise ValueError(f"Table '{name}' does not exist.")
        self._catalog[name] = metadata
        self._save()

    def delete_table(self, name: str) -> None:
        if name in self._catalog:
            del self._catalog[name]
            self._save()

    def table_exists(self, name: str) -> bool:
        return name in self._catalog

    def dispatch_storage(self, name) -> BaseStorage:
        """
        Factory method to dispatch correct storagelayer based on 
        TableMetadata storage format

        name (str): Table name
        """
        meta = self.get_table(name)
        if not meta:
            raise ValueError(f"Table '{name}' not found.")
        
        fmt = meta["storage"]["format"]
        path = meta["storage"]["path"]

        match fmt:
            case "parquet":
                return ParquetDataStorage(path)
            case _:
                return ValueError(f"Unsupported storage format: {fmt}")        