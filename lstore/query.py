from lstore.table import Table
from lstore.index import Index
from lstore.record import Record
from lstore.config import *
from lstore.helpers import *
from copy import deepcopy
from math import ceil

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified key
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, key):

        rid = self.table.record_does_exist(key)
        if rid == None:
            return False
        
        # Update delete value to true in page directory
        self.table.page_directory[rid]["deleted"] = True
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def _check_values_are_valid(self, list_of_values) -> bool:
        for val in list_of_values:
            if val < 0:
                return False
            elif ceil(val.bit_length() / 8.0) >= 8:
                return False
            elif not isinstance(val, int):
                return False
            else:
                continue

        return True       
            
    def insert(self, *columns):
        unique_identifier = columns[0]
        columns_list = list(columns)
        if len(columns_list) != self.table.num_columns:
            return False
        if not self._check_values_are_valid(columns_list):
            return False
        if self.table.record_does_exist(key=unique_identifier) != None:
            return False

        blank_schema_encoding = 0
        new_rid = self.table.new_rid()
        new_record = Record(key=unique_identifier, rid=new_rid, schema_encoding=blank_schema_encoding, column_values=columns_list)
        did_successfully_write = self.table.write_new_record(record=new_record, rid=new_rid)

        if did_successfully_write:
            return True
        else:
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
        if column > self.table.num_columns or column < 0:
            return False
        if len(query_columns) != self.table.num_columns:
            return False
        for value in query_columns:
            if value != 0 and value != 1:
                return False

        valid_rid = self.table.record_does_exist(key=key)
        if valid_rid == None:
            return False

        selected_record = self.table.read_record(rid=valid_rid)
        # print("SCHEMA 2", selected_record.meta_data[SCHEMA_ENCODING_COLUMN])
        if selected_record == False:
            return False

        return_list = []
        filtered_record_list = []
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                filtered_record_list.append(selected_record.user_data[i])
            else: # 0 specifies returning None in place of column value
                filtered_record_list.append(None)
        selected_record.user_data = filtered_record_list
        return_list.append(selected_record)
        return return_list

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # TODO : Discuss snapshots for future milestones
        # all updates will go to the tail page

        columns_list = list(columns)
        if len(columns_list) != self.table.num_columns:
            return False

        valid_rid = self.table.record_does_exist(key=key)
        if valid_rid == None:
            return False
        
        current_record = self.table.read_record(rid=valid_rid) # read record need to give the MRU
        # print("current_record", current_record.all_columns)
        schema_encoding_as_int = current_record.all_columns[SCHEMA_ENCODING_COLUMN]
        current_record_data = current_record.user_data
        # print('schema as int ', schema_encoding_as_int)
        for i in range(len(columns)):
            # print(f"colummns[i] {i}", columns[i])
            if columns[i] == None: 
                if not get_bit(value=schema_encoding_as_int, bit_index=i): # bit is 0, never been updated before, most updated entry is in base pages
                    # print(f'NOT @ i = {i}; set_bit == {set_bit(value=schema_encoding_as_int, bit_index=i)}')
                    current_record_data[i] = 0
                else:
                    # print(f' CON @ i = {i}; set_bit == {set_bit(value=schema_encoding_as_int, bit_index=i)}')
                    continue
            else:
                # print(f'ELSE @ i = {i}; set_bit == {set_bit(value=schema_encoding_as_int, bit_index=i)}')
                schema_encoding_as_int = set_bit(value=schema_encoding_as_int, bit_index=i)
                current_record_data[i] = columns[i]
        
        new_tail_record = Record(key=key, rid=valid_rid, schema_encoding=schema_encoding_as_int, column_values=current_record_data)

        return self.table.update_record(updated_record=new_tail_record, rid=valid_rid)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        record_found = False
        for pr in self.table.book:
            # for every base page in the page range
            for bp in pr.pages:
                # for every value in the KEY_COLUMN of the base page
                for i in range(ENTRIES_PER_PAGE):
                    key = bp.columns_list[KEY_COLUMN].read(i)
                    # If the key is between start_range and end_range inclusively,
                    # read the record and sum at the aggregate_column_index
                    if key >= start_range and key <= end_range:
                        rid = bp.columns_list[RID_COLUMN].read(i)
                        record = self.table.read_record(rid)
                        data_columns = record.user_data
                        sum += data_columns[aggregate_column_index]
                        record_found = True
        
        if not record_found:
            return False
        
        return sum

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

