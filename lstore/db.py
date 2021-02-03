from lstore.table import Table

class Database():

    def __init__(self):
        # store tables in a dictionary
        self.tables = {}
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name: str, num_columns: int, key: int) -> Table:
        table = Table(name, num_columns, key)
        self.tables[name] = table # TODO: check if name is best way to map
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        self.tables[name] = None
        return f"You have successfully dropped the table: {name}"

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
