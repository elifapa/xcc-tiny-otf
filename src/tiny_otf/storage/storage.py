from abc import ABC, abstractmethod
from pathlib import Path
import pyarrow.dataset as ds
import pandas as pd
from datetime import datetime

class BaseStorage(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, partition_date: datetime): pass

    @abstractmethod
    def read(self, columns: list[str] | None, limit: int | None = None) -> pd.DataFrame: pass

class ParquetDataStorage(BaseStorage):
    def __init__(self, storage_path: str, engine:str = "pyarrow"):
        self.storage_path = Path(storage_path)
        self.engine = engine

    def _get_files_in_dir(self) -> list:
        return list(self.storage_path.rglob("*.parquet"))
    
    def _n_files_in_dir(self) -> int:
        return len(self._get_files_in_dir())
    
    def write(self, 
              df: pd.DataFrame, 
              partition_date: datetime):
        
        path = self.storage_path / partition_date.strftime('%Y-%m-%d')
        path.mkdir(parents=True, exist_ok=True)
        file_name = f"raw_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
        df.to_parquet(path / file_name, 
                      index=False, 
                      engine = self.engine)
        print(f"{len(df)} rows are inserted into  {self.storage_path} successfully.")

    def read(self, columns: list[str] | None, limit: int | None = None) -> pd.DataFrame:
        """
        Read all the .parquet files in the table data directory
        and return pandas DataFrame (optionally limit the data and select columns)
        """
        table_path = self.storage_path
        nb_files = self._n_files_in_dir()

        if not nb_files>0:
            raise FileNotFoundError(f"No parquet files found for table {table_path}")
        
        print(f"Number of files under `{table_path}`: {nb_files}")

        dataset = ds.dataset(table_path, format="parquet")

        if columns:
            print("Selected columns: ", columns)
            table = dataset.to_table(columns=columns)
        else:
            table = dataset.to_table()

        if limit is not None:
            table = table.slice(0, limit)

        return table.to_pandas()
