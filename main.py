import os
from core.postgres import pg_cursor, pg_execute, pg_create_table, pg_execute_batch
from core.csv_data import read_csv_file

column_insert = {
    "employees": {
        "column_name": "employee_id varchar(8), branch_id varchar(8), salary float, join_date date, resign_date date",
        "list_column": "employee_id, branch_id, salary, join_date, resign_date"
    },
    "timesheets": {
        "column_name": "timesheet_id varchar(8), employee_id varchar(8), date date, checkin varchar(8), checkout varchar(8)",
        "list_column": "timesheet_id, employee_id, date, checkin, checkout"
    }
}

def insert_data_csv(column_insert, table_name):
    """
    Insert data from a CSV file into a PostgreSQL table.

    Args:
        column_insert (dict): Dictionary containing column information for different tables.
        table_name (str): Name of the table to insert data into.

    """

    # Get column from column insert
    column_name = column_insert[table_name]['column_name']
    column_insert = column_insert[table_name]['list_column']

    # Get the parent directory path
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(parent_dir, f"source\{table_name}.csv")
    csv_data = read_csv_file(file_path)

    connection_dwh = pg_cursor('postgresql_dwh')
    pg_create_table(connection_dwh, table_name, column_name )
    pg_execute_batch(connection_dwh, csv_data, table_name, column_insert)

if __name__ == '__main__':

    insert_data_csv(column_insert=column_insert, table_name='employees')
    insert_data_csv(column_insert=column_insert, table_name='timesheets')

    parent_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(parent_dir, f"query\\table_salary_hour.sql")
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {file_path}") from e

    connection_dwh = pg_cursor('postgresql_dwh')
    pg_execute(connection_dwh, sql_query)
