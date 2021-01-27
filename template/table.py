from template.page import *
from template.index import Index
from template.config import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Base_page:
        # Array of Pages num_columns long plus schema and indirections columns
        # size() - 2 = schema column;  size() - 1 indirection column
        def __init__(self, num_columns):
            self.columns = [Page() for i in range(num_columns + 2)] 
    
class Page_range:
    # Array of Base_pages based on Const.BASE_PAGE_COUNT
    def __init__(self, num_columns):
        self.pages = [Base_page(num_columns) for i in range(BASE_PAGE_COUNT)]

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.book = [Page_range(num_columns)] # Initialize with a single PageRange
        self.num_records = 0
        pass

    def __merge(self):
        pass
    
    def new_rid(self):
        rid = self.num_records + 1
        self.num_records += 1
        self.page_directory[rid] = _new_rid_dict()

    def _new_rid_dict() -> dict:

        rid = {
            'page_range': '',
            'base_page': '',
            'base_index': '',
            'tail_page': None,
            'tail_index': None,
            'delete': False,
            'updated': False
        }
        
        return rid