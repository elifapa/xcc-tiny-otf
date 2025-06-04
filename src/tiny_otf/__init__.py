from tiny_otf.sql_parser import SqlParser
from tiny_otf.engine import TinyEngine

def main() -> None:
    print("Hello from tiny-otf!")

    engine = TinyEngine()
    sqls = [
        "create table test_table (first_name VARCHAR, last_name VARCHAR, age INT)",
        "insert into test_table (first_name, last_name, age) values('gary', 'clark', 34), ('rick', 'vergunst', 28)",
        "select * from test_table",
        "select first_name from test_table",
        "select foo, baz from test_table",
        # "CREATE TABLE employee ( emp_id INT, emp_name VARCHAR, hire_date DATE, salary DOUBLE, is_active BOOLEAN );",
        # "INSERT INTO employee VALUES  (1, 'Alice', DATE '2022-01-15', 75000.50, true), (2, 'Bob', DATE '2021-07-01', 64000.00, false);"
    ]

    for sql in sqls:
        try:
            print("Processing sql:\n ", sql)
            my_plan = SqlParser("presto", sql).to_plan()
            result = engine.execute(my_plan)
            #result = engine._execute_select(my_plan)
            print("Query result: \n", result)
            print("\n")
        except Exception as e:
            print(f"Error: {e}\n")


    # engine.query(
    #     "create table test_table (first_name TEXT, last_name TEXT, age INT) using iceberg",
    # )
    # engine.query(
    #     "insert into test_table (first_name, last_name, age) values('gary', 'clark', 34), ('rick', 'vergunst', 28)",
    # )
    # engine.query("select * from test_table")