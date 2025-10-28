# OOP Design
| Class                | Responsibility                                 |
| -------------------- | ---------------------------------------------- |
| TableMetadata         | Read/write metadata (currently JSON file), no SQL or validation      |
| SqlParser            | Dispatches SqlPlans based on sqlglot query type (Create, Select, Insert) |
| SqlPlans             | Parsed representation of SQL intent (from sqlglot expression) |
| TinyEngine           | Main orchestrator, runs validation + execution |

# TODO  
**Implement time travel** 

**Implement SELECT in SelectPlan and TinyEngine**  
1- read all the .parquet files based on storage path in table metadata ✅  
2- Return first n rows in terminal  ✅   
3- Return only selected columns ✅  
4- Return all columns if select * ✅  


**Insert/Select schema validation**:  
1- table exists check ✅  (S+I)  
2- column names exist  (S+I)  
3- column types check (I) --> now, I want to improve this by having a mapping of dtypes between what is given in the SQL and what pandas dataframe expects.  (SQL_TO_PANDAS_TYPES)  
4- Primary key check (I) --> if PK is defined, duplicate records should not be inserted.  

**Storage**:  
1- INSERT: Optional dt/ts table column which drives filesystem partition. If no column given, fallback to insert time.  
    Figure out a way to consolidate the parquet files based on partition & max size (default = day)    


**Extend sqlplan**: CTAS, UPDATE table (?), SELECT from subquery   