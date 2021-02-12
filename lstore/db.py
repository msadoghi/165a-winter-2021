from lstore.table import Table
from lstore.bufferpool import *
import os

class Database():

    def __init__(self):
        # store tables in a dictionary
        self.tables = {}
        self.bufferpool = Bufferpool()
        self.root_name = None
        pass

    def open(self, path):
        if os.path.isdir(path):
            self.root_name = path
            print(f"Directory found")
        else:
            os.mkdir(path)
            self.root_name = path
            print(f"Directory created {path}")


    def close(self):
        # go through every table and save the page directorys
        for table_info in self.tables.values():
            print("table_info", table_info)
            table = table_info.get("table")
            table_name = table_info.get("name")
            did_close = table.close_table_page_directory()
            if not did_close:
                # TODO raise exception
                print(f"Could not close the page directory: {table_name}")
                return False
        
        return True

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name: str, num_columns: int, key: int) -> Table:

        table_path_name = f"{self.root_name}/{name}"
        if os.path.isdir(table_path_name):
            # TODO let the user know this name is already taken
            print(f"Sorry the name {name} is already taken")
            return False
        else:
            os.mkdir(table_path_name)
        
        print("table_path_name", table_path_name)
        table = Table(name, num_columns, key, path=table_path_name)
        self.tables[name] = {
            "name": name,
            "table_path_name": table_path_name,
            "num_columns": num_columns,
            "key": key,
            "table": table
        }
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            table_directory = self.tables[name]["table_path_name"]
            if os.path.isdir(table_directory):
                os.rmdir(table_directory)
                del self.tables[name]
                return True
            else:
                return False
        else:
            return False

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
