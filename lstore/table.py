from lstore.page import *
from lstore.index import Index
from lstore.config import *
from lstore.record import *
from lstore.helpers import *
from time import time
import math
import json
import os
from inspect import currentframe, getframeinfo

frameinfo = getframeinfo(currentframe())

'''
              *** Table Diagram ***

    -----------------------------------------
    |       Table: Holds Page_range(s)      |
    |   ------   ------   ------   ------   |
    |   | PR |   | PR |   | PR |   | PR |   |
    |   ------   ------   ------   ---|--   |
    |                  ...            |     |
    ----------------------------------|------
                                      |---------|  
                                                |
    -----------------------------------------   |
    |     Page_range: Holds Base_page(s)    |   |
    |   ------   ------   ------   ------   |<--|
    |   | BP |   | BP |   | BP |   | BP |   |
    |   ------   ------   ------   ------   |
    |   ------   ------   ------   ------   |
    |   | BP |   | BP |   | BP |   | BP |   |
    |   ------   ------   ------   ------   |
    |   ------   ------   ------   ------   |
    |   | BP |   | BP |   | BP |   | BP |   |
    |   ------   ------   ------   ------   |
    |   ------   ------   ------   ------   |
    |   | BP |   | BP |   | BP |   | BP |   |
    |   ------   ------   ------   ---|---  |
    |                  ...            |     |
    ----------------------------------|------
                                      |--------|         
                                               |                                            
    ----------------------------------------   |                                   
    |       Base_page: Holds Page(s)       |<--|    
    |   -----   -----   -----   -----      |    
    |   | P |   | P |   | P |   | P |      |
    |   |   |   |   |   |   |   |   |  ... |
    |   |   |   |   |   |   |   |   |      |
    |   -----   -----   --|--   -----   |  |                     Each Base_page has a
    ----------------------|-------------|---                list of Tail_page(s) for updates
                          |             |               ----------------------------------------
                          |             |-------------->|       Tail_page: Holds Page(s)       |
                          |                             |   -----   -----   -----   -----      | 
                          |                             |   | P |   | P |   | P |   | P |      | 
                          |                             |   |   |   |   |   |   |   |   |  ... |
                          |                             |   |   |   |   |   |   |   |   |      |
                          |                             |   -----   -----   -----   -----      |
                          |                             ----------------------------------------
                          |----------------------|
                                                 |
    -------------------------------------        |       
    |   Page: Array of 8 Byte Integers  |        |
    |     These are the data columns    |<-------|
    |   -----------------------------   | 
    | 0 |      8 - byte Integer     |   |   This mapping is provide in page.py
    |   -----------------------------   | 
    | 1 |      8 - byte Integer     |   |
    |   -----------------------------   | 
    | 2 |      8 - byte Integer     |   |
    |   -----------------------------   | 
    | 3 |      8 - byte Integer     |   |
    |   -----------------------------   | 
    | - |            ...            |   |
    |   -----------------------------   | 
    -------------------------------------   

'''

class BasePage:
    '''
    :param tail_page_list: list      Stores a list of all Tail_Page objects associated with the Base_page
    :param columns_list: list        Stores a list of all Page objects for the Base_page, these are the data columns
    :param tail_page_count: int      An integer that holds the current amount of tail pages the Base_page has
    :param pr_key: int               Holds the integer key of the parent Page Range
    :param key: int                  Holds the integer key of itself as it maps to the Parent Page_range list
    '''
    def __init__(self, num_columns: int, parent_key: int, bp_key: int):
        # Create a list of Physical Pages num_columns long plus Indirection, RID, TimeStamp, and Schema columns
        self.columns_list = [Page(column_num=i) for i in range(num_columns + META_COLUMN_COUNT)]
        self.pr_key = parent_key
        self.key = bp_key
    

class TailPage:
    '''
    :param columns_list: list       Stores a list of all Page objects for the Tail_page, these are the data columns
    :param bp_key: int              Holds the integer key of the parent Base_page
    '''
    def __init__(self, num_columns: int, key: int):
        self.key = key
        # Create a list of Physical Pages num_columns long plus Indirection, RID, TimeStamp, and Schema columns
        self.columns_list = [Page(column_num=i) for i in range(num_columns+ META_COLUMN_COUNT)]
        
    
class PageRange:
    '''
    :param pages: list      Stores a list of all Base_page objects in the Page_range
    :param table_key: int   Holds the integer key of the parent Table
    :param key: int         Holds the integer key of the Page_range as it is mapped in the parent Table list
    '''
    def __init__(self, num_columns: int, parent_key: int, pr_key:int):
        self.table_key = parent_key
        self.num_columns = num_columns
        self.key = pr_key
        self.num_tail_pages = 1
        self.num_tail_records = 0
        # Array of Base_pages based on Const.BASE_PAGE_COUNT
        self.base_pages = [BasePage(num_columns=num_columns, parent_key=pr_key, bp_key=i) for i in range(BASE_PAGE_COUNT)]
        self.tail_pages = [TailPage(num_columns=num_columns, key=0)]
    

    def create_new_tail_page(self) -> int:
        '''
        This function creates a new Tail_page for a PageRange and returns its index for Base_page.tail_page_list
        '''

        # length of a 0 - indexed list will return the appropriate index that the tail page will reside at 
        # in PageRange.tail_pages
        tp_index = len(self.tail_pages)
        new_tail_page = TailPage(num_columns=self.num_columns, key=tp_index)
        self.tail_pages.append(new_tail_page)

        return tp_index
    
        
class Table:
    """
    :param name: string             Name of the Table
    :param num_columns: int         Number of data columns in the table
    :param key: int                 Integer key value associated with the table
    :param page_directory: dict     Mapping of a 'RID' to the record's location
    :param index: Index             Holds created index for the table #not presently implementing for MS1
    :param book: list               List of Page_range objects, Page_range >> Base_page >> Page
    :param num_records: int         Number of records in the table
    :param num_columns: dict        dict of names associated with columns according to column keys
    """
    def __init__(self, name, num_columns, key, path=None):
        self.name = name
        self.table_path = path
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_page_ranges = 0
        self.page_range_data = {}
        self.page_ranges = [PageRange(num_columns=num_columns, parent_key=key, pr_key=0)] # Initialize with a single PageRange
        self.page_ranges_in_disk = self._allocate_page_range_to_disk()
        self.num_records = 0
        self.num_base_records = 0
        self.num_tail_records = 0
        self.column_names = { 
            0: 'Indirection',
            1: 'RID', 
            2: 'Base_RID',
            3: 'Timestamp',
            4: 'Schema'
        }

    def _allocate_page_range_to_disk(self):
        print(self.table_path)
        page_range_path_name = f"{self.table_path}/page_range_{self.num_page_ranges}"
        if os.path.isdir(page_range_path_name):
            # TODO page range not incremeneted errot
            print("Page range was not incrememnted")
            return False
        else:
            os.mkdir(page_range_path_name)
            for i in range(BASE_PAGE_COUNT):
                base_page_file = open(f"{page_range_path_name}/base_page_{i}.bin", "wb")
                physical_page = bytearray(PAGE_SIZE)
                base_page_file.write(physical_page)
                base_page_file.close()
            
            tail_page_directory_path_name = f"{page_range_path_name}/tail_pages"
            os.mkdir(tail_page_directory_path_name)

            self.page_range_data[self.num_page_ranges] = {
                "tail_page_count": 0,
                "num_tail_records": 0
            }
            
            tail_page_count = self.page_range_data[self.num_page_ranges].get("tail_page_count")
            self._allocate_new_tail_page(self.num_page_ranges, tail_page_count)
            
            self.num_page_ranges += 1
    
    def _allocate_new_tail_page(self, num_page_ranges, tail_page_count):
            new_tail_file = open(f"page_range_{num_page_ranges}/tail_page_{tail_page_count}.bin", "wb")
            physical_page = bytearray(PAGE_SIZE)
            new_tail_file.write(physical_page)
            new_tail_file.close()
            self.page_range_data[self.num_page_ranges]["tail_page_count"] += 1

    def __merge(self):
        pass

    def save_table_data(self):
        table_data = {
            "name": self.name,
            "key": self.key,
            "table_path": self.table_path,
            "num_columns": self.num_columns,
            "num_records": self.num_records,
            "num_base_records": self.num_base_records,
            "num_tail_records": self.num_tail_records,
            "column_names": self.column_names,
            "num_page_ranges": self.num_page_ranges,
            "page_range_data": self.page_range_data
        }
        self.page_directory["table_data"] = table_data
    
    def close_table_page_directory(self):
        self.save_table_data()
        page_directory_as_json = json.dumps(self.page_directory)
        # TODO make sure this opens and writes, else should return false
        page_directory_file = open(f"{self.table_path}/page_directory.json", "w")
        page_directory_file.write(page_directory_as_json)
        page_directory_file.close()
        return True

    def new_base_rid(self) -> int:
        '''
        Function that creates a new RID, increments the amount of records in the table,
        then creates a RID dict that is mapped in the Table page_directory.
        '''
        rid = self.num_records
        self.num_records += 1
        self.page_directory[rid] = self.__new_base_rid_dict()
        self.num_base_records += 1

        return rid
    

    def __new_base_rid_dict(self) -> dict:
        '''
        Helper function that returns a dict object holding values associated with a record's RID for use
        in the Table page_directory. Values not relavent to a particular record should keep the None value.

        :param page_range: int      Integer value associated with the record's Page_range index in the Table
        :param base_page: int       Integer value associated with the record's Base_page index within the Page_range
        :param base_index: int      Integer value associated with the record's Page index within the Base_page
        :param deleted: bool        Specifies whether the record was deleted
        '''
        relative_rid = self.num_base_records
        page_range_index = math.floor(relative_rid / ENTRIES_PER_PAGE_RANGE)
        index = relative_rid % ENTRIES_PER_PAGE_RANGE
        base_page_index = math.floor(index / ENTRIES_PER_PAGE)
        physical_page_index = index % ENTRIES_PER_PAGE

        # Check if current page range has space for another record
        if page_range_index > len(self.page_ranges)-1:
            self.create_new_page_range()

        record_info = {
            'page_range': page_range_index,
            'base_page': base_page_index,
            'page_index': physical_page_index,
            'deleted': False,
            'is_base_record': True
        }
        
        return record_info
    

    def new_tail_rid(self, page_range_index: int) -> int:
        '''
        Function that creates a new TID, increments the amount of records in the Base_page,
        then creates a TID dict that is mapped in the BP tail_page_directory.
        '''
        rid = self.num_records
        self.num_records += 1
        self.page_directory[rid] = self.__new_tail_rid_dict(page_range_index=page_range_index)
        self.page_ranges[page_range_index].num_tail_records += 1
        self.num_tail_records += 1

        return rid


    def __new_tail_rid_dict(self, page_range_index: int) -> dict:
        '''
        Helper function that returns a dict object holding values associated with a record's RID for use
        in the Table page_directory. Values not relavent to a particular record should keep the None value.

        :param tid: int             Integer value of the Tail Identification
        :param tail_page: int       Integer value associated with the record's Tail_page index within the Base_page
        :param page_index: int      Integer value associated with the record's Page index within the Tail_page
        '''
        relative_rid = self.page_ranges[page_range_index].num_tail_records
        tail_page_index = math.floor(relative_rid / ENTRIES_PER_PAGE) 
        physical_page_index = relative_rid % ENTRIES_PER_PAGE

        # Check if current PageRange needs another TailPage allocated
        if tail_page_index > len(self.page_ranges[page_range_index].tail_pages)-1:
            self.page_ranges[page_range_index].create_new_tail_page()
        
        rid_dict = {
            'page_range': page_range_index,
            'tail_page': tail_page_index,
            'page_index': physical_page_index,
            'is_base_record': False
        }

        return rid_dict


    def __get_last_base_page_record_location(self) -> dict:
        '''
        Helper function that returns a dict of the memory location for a given RID
        '''
        
        page_range_index = math.floor(self.num_base_records / ENTRIES_PER_PAGE_RANGE)
        index = self.num_base_records % ENTRIES_PER_PAGE_RANGE
        base_page_index = math.floor(index / ENTRIES_PER_PAGE)
        physical_page_index = index % ENTRIES_PER_PAGE

        return { 'page_range': page_range_index, 'base_page': base_page_index, 'page_index': physical_page_index }


    def set_column_name(self, name: str, column_key: int) -> None:
        '''
        Function that maps a name to a column key in the Table column_names dict

        :param name: str            Name to map to a column key
        :param column_key: int      Integer associated with a given data column 
        '''
        try:
            self.column_names[column_key] = name
            return
        except Exception as e:
            raise ValueError(f'ERROR: Unable to assign column name. {e}')
    

    def is_page_full(self, page: Page) -> bool:
        '''
        Given a Page object this function will look at the total elements a page can hold: PAGE_SIZE/PAGE_RECORD_SIZE
        in config.py, then return True if the record count for the page is equal to that value and False otherwise.
        :param page: Page       Page object to check
        :return: bool           Returns True if page is full and False otherwise
        '''

        total_elements_possible = (PAGE_SIZE / PAGE_RECORD_SIZE)  # How many total records the page can hold
        if page.num_records == total_elements_possible:           # If the record count is equal to the total possible                                                                 
            return True                                           # the page is full
        return False


    def create_new_page_range(self) -> int:
        '''
        This function creates a new PageRange for the Table and returns its key index for the Table.page_ranges list
        '''
        
        # length of a 0 - indexed list will return the appropriate index that the pr will reside at in Table.page_ranges
        pr_index = len(self.page_ranges)
        new_page_range = PageRange(num_columns=self.num_columns, parent_key=self.key, pr_key= pr_index)
        self.page_ranges.append(new_page_range)

        return pr_index
    

    def __find_next_open_tail_record(self, base_page: BasePage) -> dict:
        '''
        finds the next available tail page record slot and returns a dict with the indices, 
        and creates a new tail page if needed
        '''
        tp = 0
        # for each tail page associated with the base page
        for tail_page in base_page.tail_page_list:
            # for each element in the indirection page column in the tail page
            for i in range(tail_page.columns_list[INDIRECTION]):
                # if there is no data the record is open
                if tail_page.columns_list[INDIRECTION].read[i] == 0:   # TODO: look at different value maybe, how do we want oldest update to look?
                    # return the index to insert the update
                    return { 'tail_page': tp, 'tail_page_index': i }
        tp += 1

        # if we get here that means we did not find an open record slot in the tail pages for the base page,
        # and we need to create a new tail page

        new_tp_index = self.create_new_tail_page(base_page=base_page)

        return { 'tail_page': new_tp_index, 'tail_page_index': 0 }
    

    def __find_next_open_base_record(self) -> dict:
        '''
        finds the next available base page record slot and returns a dict with the indices, 
        and creates a new page range if needed
        '''
        pr_index = 0
        bp_index = 0
        # for every page range in the table
        for pr in self.book:
            # for every base page in the page range
            for bp in pr.pages:
                # for every value in the RID_COLUMN of the base page
                for i in range(bp.columns_list[RID_COLUMN]):
                    # If it is None then we found an open slot
                    if self.book[pr_index].pages[bp_index].columns_list[RID_COLUMN].read(i) is None:
                        return { 'page_range': pr_index, 'base_page': bp_index, 'page_index': i }
            bp_index += 1
        pr_index += 1

        # If we're here then we went through every base page in the page ranges we have for the table and did not find an open slot
        # so we need to create a new page range

        new_pr_index = self.create_new_page_range()
        return { 'page_range': new_pr_index, 'base_page': 0, 'page_index': 0 }


    def write_new_record(self, record: Record, rid: int) -> bool:
        '''
        This function takes a newly created rid and a Record and finds the appropriate base page to insert it to and updates
        the rid value in the page_directory appropriately
        '''

        record_info = self.page_directory.get(rid)
        pr = record_info.get('page_range')
        bp = record_info.get('base_page')
        pi = record_info.get('page_index')

        for i in range(len(record.all_columns)):
            value = record.all_columns[i]
            self.page_ranges[pr].base_pages[bp].columns_list[i].write(value, pi)

        return True


    def update_record(self, updated_record: Record, rid: int) -> bool:
        '''
        This function takes a Record and a RID and finds the appropriate place to write the record and writes it
        '''

        # print('--- UPDATING ---')
        rid_info = self.page_directory.get(rid)
        pr = rid_info.get('page_range')
        bp = rid_info.get('base_page')
        pp_index = rid_info.get('page_index')
        
        old_indirection_rid = self.page_ranges[pr].base_pages[bp].columns_list[INDIRECTION].read(pp_index)

        new_update_rid = self.new_tail_rid(page_range_index=pr)
        new_rid_dict = self.page_directory.get(new_update_rid)

        new_pr = new_rid_dict.get('page_range')
        new_tp = new_rid_dict.get('tail_page')
        new_pp_index = new_rid_dict.get('page_index')

        # print(f'pr = {pr} bp = {bp} pp_index = {pp_index} old_tid = {old_tid} new_tid = {new_tid} new_tid_dict = {new_tid_dict}')
        
        updated_record.all_columns[INDIRECTION] = old_indirection_rid
        updated_record.all_columns[RID_COLUMN] = new_update_rid

        # print(f'new_tp = {new_tp} new_pp_index = {new_pp_index}')
    
        for i in range(len(updated_record.all_columns)):
            # print(f'@ i = {i}; all_columns[{i}] = {updated_record.all_columns[i]}')
            value = updated_record.all_columns[i]
            self.page_ranges[new_pr].tail_pages[new_tp].columns_list[i].write(value, new_pp_index)
            # print(f'read = {self.book[pr].pages[bp].tail_page_list[new_tp].columns_list[i].read(new_pp_index)}')
        
        updated_schema = updated_record.all_columns[SCHEMA_ENCODING_COLUMN]
        # print(f'updated_schema = {updated_schema}')
        wrote_ind = self.page_ranges[pr].base_pages[bp].columns_list[INDIRECTION].write(value=new_update_rid, row=pp_index)
        # print(f'wrote_ind = {wrote_ind}')
        # print(f'read new_tid = {self.book[pr].pages[bp].columns_list[INDIRECTION].read(pp_index)}')
        wrote_schema = self.page_ranges[pr].base_pages[bp].columns_list[SCHEMA_ENCODING_COLUMN].write(value=updated_schema, row=pp_index)
        # print(f'wrote_schema = {wrote_schema}')
        # print(f'read updated_schema = {self.book[pr].pages[bp].columns_list[SCHEMA_ENCODING_COLUMN].read(pp_index)}')

        # print('----- EXITING UPDATE ------')
        return True


    def read_record(self, rid) -> Record:
        '''
        :param rid: int             RID of the record being read
        :return Record: Record      Returns the MRU Record associated with the ris

        '''
        # print('**** Reading Record *****')
        record_info = self.page_directory.get(rid)
        #check if updated value is false
        pg = record_info.get("page_range")
        bs = record_info.get("base_page")
        pp_index = record_info.get("page_index")
        all_entries = []
        
        indirection_rid = self.page_ranges[pg].base_pages[bs].columns_list[INDIRECTION].read(pp_index)

        # print(f'pg_range = {pg_range} bs_page = {bs_page} pg_index = {pg_index}')
        # print(f'indirection_tid = {indirection_tid}')

        for col in range(self.num_columns + META_COLUMN_COUNT):
            entry = self.page_ranges[pg].base_pages[bs].columns_list[col].read(pp_index)
            # print(f'Entry at col {col} = {entry}')
            all_entries.append(entry)
        key = all_entries[KEY_COLUMN]
        schema_encode = all_entries[SCHEMA_ENCODING_COLUMN]
        user_cols = all_entries[KEY_COLUMN: ]
        # print(f'key = {key} schema_encode = {schema_encode} user_cols = {user_cols}')

        if not schema_encode:
            # print('NOT SCHEMA')
            return Record(key= key, rid = rid, schema_encoding = schema_encode, column_values = user_cols)
        else:
            # print('SCHEMA')
            ind_dict = self.page_directory.get(indirection_rid)
            tail_page = ind_dict.get('tail_page')
            tp_index = ind_dict.get('page_index')
            column_update_indices = []
            # print(f'ind_dict = {ind_dict} tail_page = {tail_page} tp_index = {tp_index}')

            for i in range(KEY_COLUMN, self.num_columns + META_COLUMN_COUNT):
                # print(f'@ i = {i}')
                if get_bit(schema_encode, i - META_COLUMN_COUNT):
                    # print(f'APPEND')
                    column_update_indices.append(i)
            
            # print(f'column_update_indices = {column_update_indices}')
            for index in column_update_indices:
                user_cols[index - META_COLUMN_COUNT] = self.page_ranges[pg].tail_pages[tail_page].columns_list[index].read(tp_index)
            # print(f'user_cols = {user_cols}')

        # print(f'**** RETURNING READ *****')
        return Record(key= key, rid = rid, schema_encoding = schema_encode, column_values = user_cols)


    def record_does_exist(self, key):
        '''
        Function returns RID of the an associated key if it exists and None otherwise
        '''

        # get record to find the rid assocated with the key
        last_record_info = self.__get_last_base_page_record_location()
        last_base_page = last_record_info.get("base_page")
        found_rid = None
        current_base_page = 0

        for page_range in self.page_ranges: # for each page range
            
            for base_page in page_range.base_pages: # for each base page (0-15)
                
                key_column = base_page.columns_list[KEY_COLUMN]
                
                for i in range(ENTRIES_PER_PAGE):
                    
                    entry_value = key_column.read(i)
                    if entry_value == key:
                        rid_column = base_page.columns_list[RID_COLUMN]
                        found_rid = rid_column.read(i)
                        if self.page_directory[found_rid]["deleted"]:
                            found_rid = None
                            return found_rid
                        else:
                            return found_rid
            
                current_base_page += 1
                if current_base_page > last_base_page:
                    return found_rid
        
        return found_rid

