import sqlite3


def connection_db(db_file: str) -> object: 
    """ create a database connection to the SQLite database
        :param 
            db_file: database file
        :return: 
            Connection object
    """
    try:
        return sqlite3.connect(db_file)  
    except sqlite3.Error as e:
        raise SystemExit(e)    


def query_statement(conn, query: str) -> None:
    """ execute query statement
        :param 
            conn: Connection object
            query: SQL statement
        :return: 
            None
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
    except Exception as e:
        print('Query statement error: ', e)  


def save_data(conn, data: dict, query: str) -> None:
    """ Store the data in a table
        :param 
            conn: Connection object
            query: SQL statement
        :return: 
            None
    """
    for i in range(0, len(data['Region'])):
        try:
            cursor = conn.cursor()
            cursor.execute(
                query, 
                (data['Region'][i], data['City Name'][i], data['Language'][i], data['Time (ms)'][i])
            )
            conn.commit()
        except Exception as e:
            raise SystemExit(e)    
