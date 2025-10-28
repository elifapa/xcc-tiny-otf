from sqlglot import parse_one, exp
from pathlib import Path
from tiny_otf.sql_parser import SqlParser
from tiny_otf.engine import TinyEngine
from tiny_otf.storage.storage import MinioDataStorage
import pandas as pd
from minio import Minio
import os
import io
from datetime import datetime


client =  Minio(os.getenv("MINIO_URL"), 
                     access_key= os.getenv("ACCESS_KEY"), 
                     secret_key=os.getenv("SECRET_KEY"),
                     secure=False)

data = [['gary', 'clark', '34'], ['rick', 'vergunst', '28']]
df = pd.DataFrame(data, columns=['first_name', 'last_name', 'age'])
bytes = df.to_parquet()
buffer = io.BytesIO(bytes)
path = Path("data/") / "test_table" / datetime.today().strftime('%Y-%m-%d')
file_name = f"raw_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
print(f"{path}/{file_name}")
client.put_object(
            os.getenv("BUCKET_NAME"), f"{path}/{file_name}", buffer, length=len(bytes)
        )

# sql = "INSERT INTO test_table (first_name, age) VALUES ('gary', 34)"
# stmt = parse_one(sql)
# values = stmt.expression.expressions

my_parser = SqlParser("presto", "CREATE TABLE orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)")
# my_parser = SqlParser("presto", "SELECT a.col1, a.col2, b.col3 FROM table_A a")
my_parser_2 = SqlParser("presto", "SELECT * FROM table_A")
my_parser_3 = SqlParser("presto", "INSERT INTO employee VALUES  (1, 'Alice', DATE '2022-01-15', 75000.50, true), (2, 'Bob', DATE '2021-07-01', 64000.00, false);")
# my_parser_2 = SqlParser("presto", "CREATE TABLE orders_column_aliased (order_date, total_price) AS SELECT orderdate, totalprice FROM orders")
# my_parser_3 = SqlParser("presto", "CREATE TABLE orders (  orderkey bigint,  orderstatus varchar,  totalprice double,  orderdate date)")
# parsed = my_parser.parse_sql_from_string(sql_statement='SELECT * FROM table_A')        

# print(dir(my_parser.parsed_sql))
print(repr(my_parser.to_plan()))
print("\n\n")
print(repr(my_parser_2.to_plan()))
print(repr(my_parser_2.to_plan().is_select_star))
print("\n\n")
print(repr(my_parser_3.to_plan()))


print(Path("./table_catalog/table_manifest.json"))
        