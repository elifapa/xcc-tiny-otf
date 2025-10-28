
import pandas as pd
from tiny_otf.sql_parser import BasePlan, CreateTablePlan, InsertPlan, SelectPlan
from tiny_otf.table_catalog.table_catalog import TableMetadata
from datetime import datetime
from tiny_otf.config import SQL_TO_PANDAS_TYPES, initialize_storage

class TinyEngine:
    def __init__(self):
        self.catalog = TableMetadata()
        self.storage = initialize_storage()

    def execute(self, plan: BasePlan):
        match plan:
            case CreateTablePlan():
                return self._execute_create(plan)

            case InsertPlan():
                return self._execute_insert(plan)
            
            case SelectPlan():
                return self._execute_select(plan)

            case _:
                raise NotImplementedError("Only CREATE, INSERT and SELECT supported for now.")

    def _execute_create(self, plan: CreateTablePlan):
        if self.catalog.table_exists(plan.table_name):
            raise ValueError(f"Table '{plan.table_name}' already exists.")
        
        table_name = plan.table_name
        columns = plan.columns
        self.catalog.add_table(table_name, columns)

        print(f"Table '{table_name}' created successfully.")

    def _execute_insert(self, 
                        plan: InsertPlan):
        
        table_name = plan.table_name
        column_names = plan.column_names
        data = plan.column_values
        dtypes = plan.inferred_types

        if not self.catalog.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist.")

        table_manifest = self.catalog.get_table(table_name)
        schema = table_manifest.get("schema", None)
        
        df = plan.to_dataframe(SQL_TO_PANDAS_TYPES)

        # Create and save files under date partitions
        # storage = self.catalog.dispatch_storage(table_name)
        self.storage.write(table_name, df, datetime.today())

    def _execute_select(self, 
                        plan: SelectPlan) -> pd.DataFrame:
        
        table_names = plan.table_names
        column_names = plan.column_names or [[]]

        print(list(zip(table_names, column_names)))

        for table_name, columns in zip(table_names, column_names):

            if not self.catalog.table_exists(table_name):
                raise ValueError(f"Table '{table_name}' does not exist.")

            meta = self.catalog.get_table(table_name)
            schema = meta.get("schema", None)
            schema_column_names = [c["name"].upper() for c in schema]

            if columns:
                invalid_cols = [col for col in columns if col.upper() not in schema_column_names]
                print("invalid_cols", invalid_cols)
                if invalid_cols:
                    raise ValueError(f"Column(s) '{invalid_cols}' do not exist in table {table_name}.")

            # storage = self.catalog.dispatch_storage(table_name)

            return self.storage.read(table_name=table_name,
                                     columns=columns, 
                                     limit=5)


    