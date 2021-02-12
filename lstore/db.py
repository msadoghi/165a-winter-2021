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
            print(f"Directory found")
        else:
            os.mkdir(path)
            self.root_name = path
            print(f"Directory created {path}")


    def close(self):
        # Anything in the bufferpool with a dirty bit needs to go back to disk
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name: str, num_columns: int, key: int) -> Table:

        table_path_name = f"{self.root_name}/{name}"
        if os.path.isdir(table_path_name):
            return False
        else:
            os.mkdir(table_path_name)

        table = Table(name, num_columns, key)
        self.tables[name] = table_path_name # TODO: check if name is best way to map
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables: 
            if os.path.isdir(self.tables[name]):
                os.rmdir(self.tables[name])
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
