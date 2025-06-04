from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Tuple
import pandas as pd
from sqlglot import parse_one, exp, errors

class SqlParser:
    """
    Dispatches SqlPlans based on sqlglot query type (Create, Select, Insert)
    """
    def __init__(self, dialect:str, sql_statement:str):
        self.dialect = dialect 
        self.sql_statement = sql_statement

    @property
    def parsed_sql(self) -> exp:
        try:
            parsed_sql = parse_one(sql=self.sql_statement, dialect=self.dialect)
        except errors.ParseError as e:
            print(e.errors)
        return parsed_sql
    
    # Dispatch related class based on parsed_sql type: Create, Insert, Select
    def to_plan(self):
        expr = self.parsed_sql
        # here we are dispatching related plans 
        # using the static method from_expr
        # use `match` for object type matching
        match type(expr):
            case exp.Create:
                # if expr.args.get("expression"):  # CTAS
                #     return CTASPlan.from_expr(expr)
                print("Dispatching create plan...")
                return CreateTablePlan.from_expr(expr)

            case exp.Insert:
                print("Dispatching insert plan...")
                return InsertPlan.from_expr(expr)

            case exp.Select:
                print("Dispatching select plan...")
                return SelectPlan.from_expr(expr)

            case _:
                raise NotImplementedError(f"Unsupported SQL type: {type(expr)}")
        
##### PLANS #####
class BasePlan(ABC):
    """
    Abstract class BasePlan is used to enforce certain function in downstream classes that inherit BasePlan.
    Currently enforced functions a.k.a abstract methods: from_expr()
    """
    @abstractmethod
    def from_expr(expr: exp) -> "BasePlan":
        ...

class DataCarryingPlan(BasePlan):
    """
    Abstract class DataCarryingPlan is used to enforce certain functions in downstream classes that interact with data.
    Currently enforced functions a.k.a abstract methods: to_dataframe()
    """
    @abstractmethod
    def to_dataframe(self) -> pd.DataFrame:
        ...

@dataclass
class CreateTablePlan(BasePlan):
    table_name: str
    columns: list[dict[str, str]]  # [{"name": "id", "type": "INT"}, {}]
    raw_expr: exp.Create


    @staticmethod
    def from_expr(expr: exp.Create) -> "CreateTablePlan":
        schema_expr: exp.Schema = expr.this
        table_expr: exp.Table = schema_expr.this
        table_name = table_expr.name

        columns = []
        for coldef in schema_expr.expressions:
            if isinstance(coldef, exp.ColumnDef):
                column_name = coldef.this.name
                column_type = coldef.args["kind"].this.value
                columns.append({"name": column_name, "type": column_type})

        return CreateTablePlan(
            table_name=table_name,
            columns=columns,
            raw_expr=expr
        )

@dataclass
class InsertPlan(DataCarryingPlan):
    table_name: str
    column_names: Optional[list[str]]
    column_values: list[list[Any]]  # row-wise values
    inferred_types: list[str]  # e.g., ['int', 'str', 'float']
    raw_expr: exp.Insert

    @staticmethod
    def _get_column_values_and_types(expr_values: exp.Values) -> Tuple[list[list[Any]], list[str]]:
        expr_list = expr_values.expressions
        # nested list of row values where row value is also a list
        values = []
        dtypes = []

        # rows
        for i, row in enumerate(expr_list):
            row_value = [] # values for a single row

            # columns
            for val in row.expressions:
                match type(val):
                    case exp.Cast:
                        row_value.append(val.this.this)
                        if i==0:
                            dtypes.append('DATE')
                    case exp.Literal:
                        row_value.append(val.this)
                        if i==0 and val.is_string:
                            dtypes.append('VARCHAR')
                        elif i==0:
                            dtypes.append('INT')
                    case exp.Boolean:
                        row_value.append(val.this)
                        if i==0:
                            dtypes.append('BOOLEAN')

                # if isinstance(val, exp.Literal):
                #     # Row values &  Infer dtypes from the first row only
                #     if val.is_string:
                #         row_value.append(val.this)
                #         if i==0:
                #             dtypes.append('VARCHAR')
                #     else:
                #         try:
                #             row_value.append(float(val.this))
                #             if i==0:
                #                 dtypes.append('FLOAT')
                #         except ValueError:
                #             row_value.append(int(val.this))
                #             if i==0:
                #                 dtypes.append('INT')
                # else:
                #     row_value.append(val.this)  # fallback for expressions

            values.append(row_value)
        print("dtypes: ", dtypes)
        print("values: ", values)
        return values, dtypes

    @staticmethod
    def from_expr(expr: exp.Insert) -> "InsertPlan":
        table_name = expr.this.this.name
        column_names = [col.name for col in (expr.this.expressions or [])]

        column_values, inferred_types = InsertPlan._get_column_values_and_types(expr.expression) # Each row is a list of values

        return InsertPlan(
            table_name=table_name,
            column_names=column_names,
            column_values=column_values,
            inferred_types=inferred_types,
            raw_expr=expr
        )
    
    def to_dataframe(self, type_mapping: dict) -> pd.DataFrame:
        """
        Create a dataframe from Insert values, mapping the data types as well.
        """
        if self.column_names != []:
            df = pd.DataFrame(self.column_values, columns=self.column_names)

            for col, col_type in zip(self.column_names, self.inferred_types):
                df[col] = df[col].astype(type_mapping[col_type])
            print(df)
        else:
            df = pd.DataFrame(self.column_values)

        return df

@dataclass
class SelectPlan(BasePlan):
    table_names: list[str]
    select_expr: exp.Select
    column_names: list[list[Any]] | None = None # List of column names for each table

    @property
    def is_select_star(self) -> bool:     
        return any(isinstance(expr, exp.Star) for expr in self.select_expr.expressions)

    @staticmethod
    def from_expr(expr: exp.Select) -> "SelectPlan":
        tables = [t.name for t in expr.find_all(exp.Table)]
        is_select_star = any(isinstance(expr, exp.Star) for expr in expr.expressions)

        if is_select_star:
            return SelectPlan(
                table_names=tables,
                select_expr=expr
            )
        else:
            column_names = [[col.name for col in expr.expressions ]]
            print("column_names", column_names)
            # column_names = expr.expressions.column.this.this
            return SelectPlan(
                table_names=tables,
                select_expr=expr,
                column_names = column_names
            )



