from lstore.table import Table
from lstore.index import Index
from lstore.record import Record
from copy import deepcopy

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):

        rid = self.table.record_does_exist(key)
        if rid == False:
            return False
        
        # Update delete value to true in page directory
        self.table.page_directory[rid]["deleted"] = True
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):

        schema_encoding = '0' * self.table.num_columns
        new_rid = self.table.new_rid()
        unique_identifier = columns[0]
        columns_list = list(columns)
        new_record = Record(key=unique_identifier, rid=new_rid, schema_encoding=schema_encoding, column_values=columns_list)
        print("new_record", new_record.all_columns)
        did_successfully_write = self.table.write_new_record(record=new_record, rid=new_rid)

        if did_successfully_write:
            return True
        else:
            # In the future : if a commit fails, we can try a second time
            return False

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :column: the index where key is stored in our table
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    # Tests: query_tests.py
    """
    def select(self, key, column, query_columns):

        for value in query_columns:
            if value != 0 and value != 1:
                raise ValueError('ERROR: query_columns list must contain 0 or 1 values.')

        validRID = self.table.record_does_exist(key=key)
        selected_record = self.table.read_record(rid=validRID)

        if selected_record == False:
            return False

        return_list = []
        filtered_record_list = []
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                filtered_record_list.append(selected_record.user_data[i])
            else: # skip past 0
                continue
        
        return_list.append(filtered_record_list)
        return return_list

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # TODO : Discuss snapshots for future milestones
        # all updates will go to the tail page
        validRID = self.table.record_does_exist(key=key)
        if not validRID:
            return False
        
        current_record = self.table.read_record(rid=validRID) # read record need to give the MRU
        updated_schema_encoding = copy.deepcopy(current_record.schema_encoding)
        updated_user_data = copy.deepcopy(current_record.user_data)
        
        for i in range(len(columns)):
            if columns[i] == None:
                continue
            else:
                updated_schema_encoding[i] = "1"
                updated_user_data[i] = columns[i]
        
        # TODO new_tid(key) -> tid
        new_tid = new_tid(key)
        new_tail_record = Record(key=key, rid=new_tid, schema_encoding=updated_schema_encoding, column_values=updated_user_data)
        # TODO update_record -> bool
        did_successfully_update = self.table.update_record(record=new_tail_record, rid=new_tid)
        if did_successfully_update:
            return True
        else:
            return False

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

