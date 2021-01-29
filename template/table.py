from template.page import *
from template.index import Index
from template.config import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Base_page:
    '''
    :param tail_page_list: list      Stores a list of all Tail_Page objects associated with the Base_page
    :param columns_list: list        Stores a list of all Page objects for the Base_page, these are the data columns
    :param tail_page_count: int      An integer that holds the current amount of tail pages the Base_page has
    '''
    def __init__(self, num_columns: int):
        # Create a starting Tail Page for updates
        self.tail_page_list = [Tail_page(num_columns)]
        # Create a list of Physical Pages num_columns long plus Indirection, RID, TimeStamp, and Schema columns
        self.columns_list = [Page() for i in range(num_columns + 4)]
        self.tail_page_count = 1


class Tail_page:
    '''
    :param columns_list: list       Stores a list of all Page objects for the Tail_page, these are the data columns
    '''
    def __init__(self, num_columns: int):
        # Create a list of Physical Pages num_columns long plus Indirection, RID, TimeStamp, and Schema columns
        self.columns_list = [Page() for i in range(num_columns + 4)]
        
    
class Page_range:
    '''
    :param pages: list      Stores a list of all Base_page objects in the Page_range
    '''
    def __init__(self, num_columns: int):
        # Array of Base_pages based on Const.BASE_PAGE_COUNT
        self.pages = [Base_page(num_columns) for i in range(BASE_PAGE_COUNT)]


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
        self.book = [Page_range(num_columns)] # Initialize with a single PageRange
        self.num_records = 0
        self.column_names = { 
            0: 'Indirection', 
            1: 'RID', 
            2: 'Time Stamp',
            3: 'Schema'
        }


    def __merge(self):
        pass
    

    def new_rid(self) -> None:
        '''
        Function that creates a new RID, increments the amount of records in the table,
        then creates a blank RID dict that is  mapped in the Table page_directory.
        '''

        rid = self.num_records + 1
        self.num_records += 1
        self.page_directory[rid] = _new_rid_dict()
    

    def _new_rid_dict(self) -> dict:
        '''
        Helper function that returns a dict object holding values associated with a record's RID for use
        in the Table page_directory. Values not relavent to a particular record should keep the None value.

        :param page_range: int      Integer value associated with the record's Page_range index in the Table
        :param base_page: int       Integer value associated with the record's Base_page index with the Page_range
        :param base_index: int      Integer value associated with the record's Page index within the Base_page
        :param tail_page: int       Integer value associated with the record's Tail_page index within the Base_page
        :param tail_index: int      Integer value associated with the record's Page index within the Tail_page

        '''

        rid = {
            'page_range': None,
            'base_page': None,
            'base_index': None,
            'tail_page': None,
            'tail_index': None,
            'delete': False,
            'updated': False
        }
        
        return rid
    

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
