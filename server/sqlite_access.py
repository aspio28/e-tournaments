import sqlite3
import os

def create_db(data_base_file_path:str):
    """ Creates a sqlite database with especified name. The name should have a full existing path
    to where the database must be constructed, including the file name. The extension '.db' is not necessary
    
    Examples:

    'data/sqlite_database.db'

    'data/sqlite_database'

    'sqlite_database'
    """
    connection = None
    try:
        if data_base_file_path[len(data_base_file_path)-3:] != '.db':
            data_base_file_path += '.db'
        connection = sqlite3.connect(data_base_file_path)
        print("Database created and Successfully Connected to SQLite")
        connection.commit()
    except sqlite3.Error as error:
        print("Error while connecting to sqlite:", error)
    finally:
        if connection:
            connection.close()
            # print("The SQLite connection is closed")

def create_table(data_base_file_path:str, table_name:str, columns_list:list):
    """Createss an sqlite table in the file especified and with the name especified in 'table_name'.
    If 'columns_list' has 0 elements the table won't be created.

    The format for columns_list is:

    A list of strings where every string is 'column_name type_in_sqlite rest_of_params_to_espicify_separated_by_spaces'
    
    Example:
            [
            'id_S INTEGER PRIMARY KEY',
            'title TEXT NOT NULL',
            'artists TEXT NOT NULL',
            'genre TEXT NOT NULL'
            ]
    """
    connection = None
    if len(columns_list) == 0:
        return False
    try:
        connection = sqlite3.connect(data_base_file_path)
        create_table_query = "CREATE TABLE IF NOT EXISTS " + table_name + ' ( ' 
        for i in range(len(columns_list)):
            column = columns_list[i]
            create_table_query = create_table_query + column
            if i != len(columns_list) -1:
                create_table_query = create_table_query + ', '
        create_table_query = create_table_query + ');'

        cursor = connection.cursor()
        # print("Successfully Connected to SQLite")
        cursor.execute(create_table_query)
        connection.commit()
        # print(f"table {table_name} in database OK")
        cursor.close()
    except sqlite3.Error as error:
        print("Error while creating a sqlite table:", error)
    finally:
        if connection:
            connection.close()
            # print("sqlite connection is closed")

def insert_rows(data_base_file_path:str, table_name:str, columns_names:str, row_tuples_tuple:tuple):
    """ Insert one or more rows in the specified table, of the specified database.
    'columns_names' is a string of the name of the columns in the table, comma separated.
    'row_tuples_tuple' is a tuple with one or more elements, where each element is a tuple 
    with the values to insert to the row. Must be ordered in the same fashion as 'colummns_names'.

    Example:

        data_base_file_path = 'spotify_db.db'

        table_name = 'songs'

        columns = 'id_S, title, artists, genre'

        rows = ((0, 'This Is Halloween', 'Marilyn Manson', 'Soundtrack'),

                (6, 'Extraordinary Girl', 'Green Day', 'Punk Rock'),

                (11, 'House of the Rising Sun', 'Five Finger Death Punch', 'Rock'))

        insert_rows(data_base_file_path, table_name, columns, rows)
    """
    ids = []
    if len(row_tuples_tuple) == 0:
        return False

    try:
        connection = sqlite3.connect(data_base_file_path)
        cursor = connection.cursor()
        # print("Connected to SQLite")

        sqlite_query = "INSERT OR REPLACE INTO " + table_name + " ( " + columns_names + " ) VALUES "
        
        value = '(' + ('?,'*(len(row_tuples_tuple[0])-1)) + '?);'
        # insert = sqlite_query + value
        # cursor.executemany(insert, row_tuples_tuple)      
        # last_id = cursor.lastrowid
        for row in row_tuples_tuple:
            insert = sqlite_query + value
            cursor.execute(insert, row)
            ids.append(cursor.lastrowid)
            # print(f"Last inserted ID: {ids[-1]}")

        connection.commit()
        # print("Values inserted successfully into the table")
        cursor.close() 
    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table:",error)
    finally:
        if connection:
            connection.close()
            # print("The sqlite connection is closed")
    return ids

def read_data(data_base_file_path:str, query:str="SELECT * from songs"):
    """ Given the path of the database file and a sqlite query, returns all rows corresponding to the query
        
        Examples:
                
                # get all data from 'songs' table:
                
                song_list = read_data('spotify_db.db', "SELECT * from songs")s
                
                
                # get a chunk of song which id is '11_dice_003' :
                
                query = "SELECT * from chunks where id_Chunk = '11_dice_003'"
                
                chunk = read_data('spotify_db.db', query)
    """
    record = None
    try:
        connection = sqlite3.connect(data_base_file_path)
        cursor = connection.cursor()
        # print("Connected to SQLite")

        cursor.execute(query)
        record = cursor.fetchall()
        cursor.close()
 
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table:", error)
    finally:
        if connection:
            connection.close()
            # print("sqlite connection is closed")
    return record
            