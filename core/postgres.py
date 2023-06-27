import psycopg2, json
from psycopg2.extras import execute_batch
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

db_config = {
    "postgresql_dwh": {
        "host":"localhost",
        "port":"5433",
        "username":"example-username",
        "password":"pass",
        "database":"db-name"
    }
}

def get_db_config(key: str):
    """
    Retrieve the database configuration based on the provided key.

    Args:
        key (str): Key to identify the database configuration in the db_config dictionary.

    Returns:
        dict: Database configuration values (host, port, username, password, database) as a dictionary.
    """

    try:
        config_key = db_config[key]
        return config_key
    except KeyError as e:
        raise KeyError(f"Invalid database connection key: {key}") from e
    
def pg_cursor(connection: str):
    """
    Create and return a PostgreSQL cursor for the specified database connection.

    Args:
        connection (str): Key to identify the database connection in the db_config dictionary.

    Returns:
        psycopg2.extensions.cursor: PostgreSQL cursor.

    """

    try:
        credentials = get_db_config(connection)
        client = psycopg2.connect(
            host=credentials['host'],
            port=credentials['port'],
            database=credentials['database'],
            user=credentials['username'],
            password=credentials['password']
        )
        client.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return client.cursor()
    except psycopg2.Error as e:
        raise psycopg2.Error("Failed to establish a database connection") from e

def pg_read(cursor: pg_cursor, sql: str):
    """
    Execute a SQL query using the provided PostgreSQL cursor and retrieve the column names and query results.

    Args:
        cursor (psycopg2.extensions.cursor): PostgreSQL cursor.
        sql (str): SQL query to execute.

    Returns:
        tuple: A tuple containing a list of column names and a list of query results.
    """

    try:
        cursor.execute(sql)
        column = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return column, data
    except psycopg2.Error as e:
        raise psycopg2.Error("Failed to execute SQL query") from e

def pg_execute(cursor: pg_cursor, sql: str):
    """
    Execute a SQL query using the provided PostgreSQL cursor.

    Args:
        cursor (psycopg2.extensions.cursor): PostgreSQL cursor.
        sql (str): SQL query to execute.
    """

    try:
        print(sql)
        return cursor.execute(sql)
    except psycopg2.Error as e:
        raise psycopg2.Error("Failed to execute SQL query") from e

def pg_create_table(cursor: pg_cursor, table_name, columns_generate_tables):
    """
    Create a table in the PostgreSQL database using the provided cursor.

    Args:
        cursor (psycopg2.extensions.cursor): PostgreSQL cursor.
        table_name (str): Name of the table to create.
        columns_generate_tables (str): Columns with data types for generating the table, separated by commas.
    """

    try:
        query_drop_table = "DROP TABLE IF EXISTS " + table_name
        query_create_table = "CREATE TABLE " + table_name + '(' + columns_generate_tables + ')'
        pg_execute(cursor, query_drop_table)
        pg_execute(cursor, query_create_table)
    except psycopg2.Error as e:
        raise psycopg2.Error("Failed to create table") from e

def pg_execute_batch(cursor, data, table_name, column_in_list):
    """
    Execute a batch insert operation to insert multiple rows into a table in the PostgreSQL database.

    Args:
        cursor (psycopg2.extensions.cursor): PostgreSQL cursor.
        data (list): List of data to be inserted.
        table_name (str): Name of the table.
        column_in_list (str): Columns to insert data into, separated by commas.

    """

    try:
        values = "VALUES({})".format(",".join(["%s" for _ in column_in_list.split(',')]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, column_in_list, values)
        execute_batch(cur=cursor, sql=insert_stmt, argslist=data)
    except psycopg2.Error as e:
        raise psycopg2.Error("Failed to execute batch insert") from e

    