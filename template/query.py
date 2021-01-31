from template.table import Table
from template.index import Index
from template.record import Record

# I think the record class shouldn't be in this file

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        # check that table exists in Database

        # Maybe we can store variables here with the status of the table in terms of available space in page range, next avilable rid, etc

        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        # key is the unique identifier that is user facing and should map to an internal rid

        # delete only flags for deletion, does not actually delete record 
        # Are flagging to delete the whole record, or just 1 rollback, or maybe we will need to support both later on?
        # 1. Find the record in our page_directory - make sure record exists
        # 2. Update delete value to true in page directory
        # 3. Change schema encoding to all zeros
        # 4. Go to tail pages and create a new record with all null values in the intries and zeroes for the scheme encoding
        # 5. Set indirection pointer of new tail record to point to SMRU
        # 6. Set indirection pointer of base page record to point to new MRU
        # Return true if successfully found record and completed steps 1-6

        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        # 0. Check if space is avalable in our current page range, if not add a new page range
        # 1. Create a new rid and figure out where this will be stored
        # 2. Insert the values into each physical page
        # 3. This should be the first instance of this record so the schema encoding will be all zeroes
        primary_key = column[0]
        new_rid = self.table.new_rid(primary_key)
        new_record = Record(primary_key, new_rid, schema_encoding, columns)
        # 4. return true if successfully inserts
        # TODO : Create a funcion that checks if our page range has space
        # TODO : Discuss if insertion will be handled within the page class
        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :column: the index where key is stored in our table
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        # 1. Go to specifed column and find the value that matches key
        # 2. Check the query_columns list for the indexes set to 1
        # 3. Check the schema encoding to see if we need to get the value from the base page or from the tail page
        # 4. Aggregate all the values by the specifed query_columns list and return
        # In milestone 1, this only returns a list with a single record
        # EX: select(9898798, 0, [0, 1, 0, 1]) -> return [[89, 76]] 89 and 76 are from column 1 and 3 respectively 
        # and 9898798 is the unique identifier (key) for that record
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # Check if column has ever been updated and create a snapshot if it has not
        # all updates will go to the tail page
        # 1. Check that key exists in column 0 else return false
        # 2. Create a new tail record, with a new tid
        # 3. We will be given a list [None, None, NewValue, ... None, NewValue]
        # 4. Update schema encoding at all columns with non None values to equal 1
        # 5. Find the MRU and make a copy
        # 6. Update base page indirection pointer to point to the new MRU
        # 7. Update tail page indirection pointer of MRU to the SMRU
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # Use index functions to sum all values specifed by aggrehate_column_index that fall between start_range and end_range
        # Note that the MRU for the specified aggregate_column_index needs to be used in the sum, so make sure to check the schema encoding
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column): # not tested in milestone 1
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

