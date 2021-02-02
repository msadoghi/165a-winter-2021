from lstore.page import *
from lstore.index import Index
from lstore.config import *
from lstore.record import *
from time import time
import math

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

class Base_page:
    '''
    :param tail_page_list: list      Stores a list of all Tail_Page objects associated with the Base_page
    :param columns_list: list        Stores a list of all Page objects for the Base_page, these are the data columns
    :param tail_page_count: int      An integer that holds the current amount of tail pages the Base_page has
    :param pr_key: int               Holds the integer key of the parent Page Range
    :param key: int                  Holds the integer key of itself as it maps to the Parent Page_range list
    '''
    def __init__(self, num_columns: int, parent_key: int, bp_key: int):
        # Create a starting Tail Page for updates
        self.tail_page_list = [Tail_page(num_columns=num_columns, parent_key=bp_key, key=0)]
        # Create a list of Physical Pages num_columns long plus Indirection, RID, TimeStamp, and Schema columns
        self.columns_list = [Page(column_num=i) for i in range(num_columns + META_COLUMN_COUNT)]
        self.tail_page_directory = {}
        self.tail_page_count = 1
        self.num_tail_page_records = 0
        self.pr_key = parent_key
        self.key = bp_key
    

    def create_new_tail_page(self) -> int:
        '''
        This function creates a new Tail_page for a Base_page and returns its index for Base_page.tail_page_list
        '''

        # length of a 0 - indexed list will return the appropriate index that the tail page will reside at 
        # in Base_page.tail_page_list
        tp_index = len(self.tail_page_list)
        num_columns = len(self.columns_list) - META_COLUMN_COUNT
        new_tail_page = Tail_page(num_columns=num_columns, parent_key=self.key, key=tp_index)
        self.tail_page_list.append(new_tail_page)

        return tp_index
    

    def new_tid(self) -> int:
        '''
        Function that creates a new TID, increments the amount of records in the Base_page,
        then creates a TID dict that is mapped in the BP tail_page_directory.
        '''
        tid = self.num_tail_page_records
        self.num_records += 1
        self.page_directory[tid] = self._new_rid_dict(tid)

        return tid


    def __new_tid_dict(self, tid: int) -> dict:
        '''
        Helper function that returns a dict object holding values associated with a record's RID for use
        in the Table page_directory. Values not relavent to a particular record should keep the None value.

        :param tid: int             Integer value of the Tail Identification
        :param tail_page: int       Integer value associated with the record's Tail_page index within the Base_page
        :param page_index: int      Integer value associated with the record's Page index within the Tail_page
        '''

        tail_page_index = math.floor(tid / ENTRIES_PER_PAGE) 
        physical_page_index = tid % ENTRIES_PER_PAGE

        # Check if current page range has space for another record
        if tail_page_index > len(self.tail_page_list)-1:
            self.create_new_tail_page()

        return { 'tail_page': tail_page_index, 'page_index': physical_page_index }


    def __tid_to_page_location(self, tid: int) -> dict:

        tail_page_index = math.floor(tid / ENTRIES_PER_PAGE) 
        physical_page_index = tid % ENTRIES_PER_PAGE

        return { 'tail_page': tail_page_index, 'page_index': physical_page_index }
        

class Tail_page:
    '''
    :param columns_list: list       Stores a list of all Page objects for the Tail_page, these are the data columns
    :param bp_key: int              Holds the integer key of the parent Base_page
    '''
    def __init__(self, num_columns: int, parent_key: int, key: int):
        self.bp_key = parent_key
        self.key = key
        # Create a list of Physical Pages num_columns long plus Indirection, RID, TimeStamp, and Schema columns
        self.columns_list = [Page(column_num=i) for i in range(num_columns+ META_COLUMN_COUNT)]
        
    
class Page_range:
    '''
    :param pages: list      Stores a list of all Base_page objects in the Page_range
    :param table_key: int   Holds the integer key of the parent Table
    :param key: int         Holds the integer key of the Page_range as it is mapped in the parent Table list
    '''
    def __init__(self, num_columns: int, parent_key: int, pr_key:int):
        self.table_key = parent_key
        self.key = pr_key
        # Array of Base_pages based on Const.BASE_PAGE_COUNT
        self.pages = [Base_page(num_columns=num_columns, parent_key=pr_key, bp_key=i) for i in range(BASE_PAGE_COUNT)]

        
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
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.book = [Page_range(num_columns=num_columns, parent_key=key, pr_key=0)] # Initialize with a single Page_range
        self.num_records = 0
        self.column_names = { 
            0: 'Indirection',
            1: 'RID', 
            2: 'Time Stamp',
            3: 'Schema'
        }


    def __merge(self):
        pass
    

    def new_rid(self) -> int:
        '''
        Function that creates a new RID, increments the amount of records in the table,
        then creates a RID dict that is mapped in the Table page_directory.
        '''
        rid = self.num_records
        self.num_records += 1
        self.page_directory[rid] = self._new_rid_dict(rid)

        return rid
    

    def __new_rid_dict(self, rid) -> dict:
        '''
        Helper function that returns a dict object holding values associated with a record's RID for use
        in the Table page_directory. Values not relavent to a particular record should keep the None value.

        :param page_range: int      Integer value associated with the record's Page_range index in the Table
        :param base_page: int       Integer value associated with the record's Base_page index within the Page_range
        :param base_index: int      Integer value associated with the record's Page index within the Base_page
        :param deleted: bool        Specifies whether the record was deleted
        :param updated: bool        Determines whether we need to scan tail pages at all
        '''
        page_range_index = math.floor(rid / ENTRIES_PER_PAGE_RANGE)
        index = rid % ENTRIES_PER_PAGE_RANGE
        base_page_index = math.floor(index / ENTRIES_PER_PAGE)
        physical_page_index = index % ENTRIES_PER_PAGE

        # Check if current page range has space for another record
        if page_range_index > len(self.book)-1:
            self.create_new_page_range()

        record_info = {
            'page_range': page_range_index,
            'base_page': base_page_index,
            'page_index': physical_page_index,
            'updated': False,
            'deleted': False
        }
        
        return record_info


    def __rid_to_page_location(self, rid: int) -> dict:
        '''
        Helper function that returns a dict of the memory location for a given RID
        '''

        page_range_index = math.floor(rid / ENTRIES_PER_PAGE_RANGE)
        index = rid % ENTRIES_PER_PAGE_RANGE
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
        This function creates a new Page_range for the Table and returns its key index for the Table.book list
        '''
        
        # length of a 0 - indexed list will return the appropriate index that the pr will reside at in Table.book
        pr_index = len(self.book)
        new_page_range = Page_range(num_columns=self.num_columns, parent_key=self.key, pr_key= pr_index)
        self.book.append(new_page_range)

        return pr_index
    

    def __get_update_write_location_info(self, rid: int) -> dict:
        '''
        This function takes a RID and finds the appropriate place to write and returns a dict of the indices
        '''
        rid_info = self.page_directory.get(rid)
        pr = rid_info.get('page_range')
        bp = rid_info.get('base_page')
        bp_index = rid_info.get('base_index')

        update_tid = self.book[pr].pages[bp].new_tid()
        update_record = self.book[pr].pages[bp].tail_page_directory.get(update_tid)

        # check if the record has been updated or not 
        if rid_info.get('updated') == False:
            location_info = {
                'mru_tid': -1,
                'mru_page': -1,         # TODO: this will be the indirection value for update, may want to evaluate how we do this or look into special null values
                'mru_page_index': -1,   # checking List element -1 may be allowed (off set from back?) and cause issues, maybe store a string or something
                'update_tid': update_tid,
                'update_page': update_record.get('tail_page'),
                'update_page_index': update_record.get('page_index')
            }
            return location_info
        
        # If we're here the record has already been updated and needs to be updated again
        # check Indirection column value for the TID associated with the RID
        indirection_tid = self.book[pr].pages[bp].columns_list[INDIRECTION].read(bp_index)
        tid_record = self.book[pr].pages[bp].tail_page_directory.get(indirection_tid)
        

        location_info = {
            'mru_tid': indirection_tid,
            'mru_page': tid_record.get('tail_page'),
            'mru_page_index': tid_record.get('page_index'),
            'update_tid': update_tid,
            'update_page': update_record.get('tail_page'),
            'update_page_index': update_record.get('page_index') 
        }

        return location_info


    def __find_next_open_tail_record(self, base_page: Base_page) -> dict:
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
                if tail_page.columns_list[INDIRECTION].read[i] is 0:   # TODO: look at different value maybe, how do we want oldest update to look?
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

        write_location = self.page_directory[rid]
        pr = write_location.get('page_range')
        bp = write_location.get('base_page')
        pi = write_location.get('page_index')

        for i in range(len(record.all_columns)):
            value = record.all_columns[i]
            self.book[pr].pages[bp].columns_list[i].write(value, pi)

        return True


    def update_record(self, record: Record, rid: int) -> bool:
        '''
        This function takes a Record and a RID and finds the appropriate place to write the record and writes it
        '''
        
        # Get info about where to write the record
        write_info = __get_update_write_location_info(rid=rid)

        rid_info = self.page_directoy.get(rid)
        pr = rid_info.get('page_range')
        bp = rid_info.get('base_page')
        bp_index = rid_info.get('base_index')

        update_tid = write_info.get('update_tid')
        update_tp = write_info.get('update_page')
        update_tp_index = write_info.get('update_page_index')

        mru_tid = write_info.get('mru_tid')
        mru_tp = write_info.get('mru_page')
        mru_tp_index = write_info.get('mru_page_index')

        # Make Indirection column of Record point to MRU
        record.all_columns[INDIRECTION] = mru_tid

        # go through every column value in the record and write it to the location
        for i in range(record.all_columns):
            value = record.all_columns[i]
            self.book[pr].pages[bp].tail_page_list[update_tp].columns_list[i].write(value, update_tp_index)

        # Update Indirection column for Base Record
        self.book[pr].pages[bp].columns_list[INDIRECTION].write(update_tid, bp_index)

        if rid_info.get('updated') == True:

            # Update Indirection columns for previous update
            self.book[pr].pages[bp].tail_page_list[mru_tp].column_list[INDIRECTION].write(update_tid, mru_tp_index)
            return True

        # set rid value to updated for future
        self.page_directory[rid].update({ 'updated': True })
        return True


    def read_record(self, rid):
        # scan column data column 0 to find the whole record
        record = Record(rid, 0, "00000", [1, 2, 3, 4, 5, 6])
        return record

    # returns rid if found else False
    def record_does_exist(self, key):
        # get record to find the rid assocated with the key
        found_rid = False
        for page_pange in self.book: # for each page range
            for base_page in page_pange.pages: # for each base page (0-15)
                key_column = base_page.columns_list[KEY_COLUMN]
                rid_column = base_page.columns_list[RID_COLUMN]
                for i in range(ENTRIES_PER_PAGE):
                    entry_value = key_column.read(i)
                    if entry_value == key:
                        found_rid = rid_column.read(i)
                        if self.page_directory[found_rid]["deleted"]:
                            found_rid = False
        
        return found_rid

    def new_tid(key) -> int:
        return 1

        # record_rid = found_record.rid
        if record_rid not in self.page_directory:
            return False
        else:
            # found key but record was deleted
            if self.page_directory[record_rid]["deleted"]:
                return False
            else: # record exists
                return record_rid

