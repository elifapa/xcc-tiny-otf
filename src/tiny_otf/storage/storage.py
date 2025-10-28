from typing import Any, Protocol
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs
import pandas as pd
from datetime import datetime
from minio import Minio
import os
import io
# from tiny_otf.config import STORAGE_PATH

class BaseStorage(Protocol):
    """Base protocol for read/write operations"""
    def write(self, df: pd.DataFrame, partition_date: datetime): pass

    def read(self, columns: list[str] | None, limit: int | None = None) -> pd.DataFrame: pass

class ClientAware(Protocol):
    """Protocol for classes with client property"""
    @property
    def client(self) -> Any:
        ...

class ThirdPartyStorage(BaseStorage, ClientAware, Protocol):
    """Combined protocol using multiple inheritance"""
    pass 

class LocalFSDataStorage(BaseStorage):
    def __init__(self, 
                 base_path: str, 
                 engine:str = "pyarrow", 
                 file_type:str = "parquet"):
        self.base_path = Path(base_path)
        self.engine = engine
        self.file_type = file_type
    
    def _get_files_in_dir(self, table_name: str) -> list:
        table_path = self.base_path / table_name
        return list(table_path.rglob("*.parquet"))
    
    def _n_files_in_dir(self, table_name: str) -> int:
        return len(self._get_files_in_dir(table_name))
    
    def write(self, 
              table_name: str,
              df: pd.DataFrame, 
              partition_date: datetime):
        
        path = self.base_path / table_name / partition_date.strftime('%Y-%m-%d')
        path.mkdir(parents=True, exist_ok=True)
        file_name = f"raw_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
        df.to_parquet(path / file_name, 
                      index=False, 
                      engine = self.engine)
        print(f"{len(df)} rows are inserted into {path} successfully.")

    def read(self, 
             table_name: str,
             columns: list[str] | None, 
             limit: int | None = None) -> pd.DataFrame:
        """
        Read all the .parquet files in the table data directory
        and return pandas DataFrame (optionally limit the data and select columns)
        """
        table_path = self.base_path / table_name
        nb_files = self._n_files_in_dir(table_name)

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

class MinioDataStorage(ThirdPartyStorage):
    def __init__(self,
                 base_path: str, 
                 is_secure=False,
                 file_type:str = "parquet"):
        self.url = os.getenv("MINIO_URL")
        self.access_key = os.getenv('ACCESS_KEY')
        self.secret_key = os.getenv('SECRET_KEY')
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.base_path = Path(base_path)
        self.secure = is_secure
        self.file_type = file_type

        print("***********Minio parameters***********",
              "\nURL:", self.url, 
              "\nSecure:", self.secure,
              "\nBucket name:", self.bucket_name,
              "\nFile type:", self.file_type)

    @property
    def client(self) -> Minio:
        return Minio(self.url, 
                     access_key=self.access_key, 
                     secret_key=self.secret_key,
                     secure=self.secure)
    
    @property
    def filesystem(self) -> fs.S3FileSystem:
        return fs.S3FileSystem(
                endpoint_override=self.url,
                access_key=self.access_key,
                secret_key=self.secret_key,
                scheme='http'
                )
    
    def create_bucket(self) -> None:
        # Make the bucket if it doesn't exist.
        found = self.client.bucket_exists(self.bucket_name)
        if not found:
            self.client.make_bucket(self.bucket_name)
            print("Created bucket", self.bucket_name, "in Minio")
        else:
            print("Bucket", self.bucket_name, "already exists")
    
    def write(self,               
              table_name: str,
              df: pd.DataFrame, 
              partition_date: datetime, 
              ):
        # TODO implement other file types 

        # create source buffer
        if self.file_type == "parquet":
            bytes = df.to_parquet()
        else:
            raise NotImplementedError(f"File type {self.file_type} is not supported yet.")
        
        buffer = io.BytesIO(bytes)
        path = self.base_path / table_name / partition_date.strftime('%Y-%m-%d')
        file_name = f"raw_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
        print(f"{path}/{file_name}")

        # load to Minio
        self.client.put_object(
            self.bucket_name, f"{path}/{file_name}", buffer, length=len(bytes)
        )
        
        print("File successfully uploaded as object", path/file_name, "to bucket", self.bucket_name)

    def read(self, 
             table_name: str,
             columns: list[str] | None = None, 
             limit: int | None = None) -> pd.DataFrame:
        
       # Read from Minio
        # response = self.client.get_object(self.bucket_name, f"{self.base_path}/{table_name}")
        # bytes = response.read()
 
        # Create DataFrame from bytes
        # buffer = io.BytesIO(bytes)

        s3_fs = self.filesystem
        if self.file_type == "parquet":
            # df = pd.read_parquet(buffer, columns=columns)
            dataset = pq.ParquetDataset(f"{self.base_path}/{table_name}", filesystem=s3_fs)
        else:
            raise NotImplementedError(f"File type {self.file_type} is not supported yet.")

        if columns:
            print("Selected columns: ", columns)
            table = dataset.to_table(columns=columns)
        else:
            table = dataset.to_table()

        if limit is not None:
            table = table.slice(0, limit)

        print(
            "File successfully downloaded as object",
            self.base_path / table_name, "from bucket", self.bucket_name,
        )
        return table.to_pandas()