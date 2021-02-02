# from lstore.table import *
from random import randint
from lstore.config import *
import math

def makeTable():
    '''
    Test used to visualize Table layout
    '''
    
    x = Table('students', 5, 1)
    print(x)
    print(x.book)
    for pr in x.book:
        print('Page Range:\n')
        print(f'{pr}\n')
        for bp in x.book[0].pages:
            print('     Base Page:\n')
            print(f'        {bp}\n')
            print('             Columns:\n')
            for column in bp.columns_list:
                # print(f'{column} : {column.data}\n')
                print(f'            {column}\n')

# makeTable()

def testInsert():
    records = {}
    for i in range(0, 1000):
        key = 92106429 + randint(0, 9000)
        while key in records:
            key = 92106429 + randint(0, 9000)
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        # query.insert(*records[key])
        # print('inserted', records[key])
    # print(records)
    print(*records[key])
    print(records[key])
# testInsert()

def __rid_to_page_location(rid: int) -> dict:
    #pr_index = 3 # len(self.book)
    # number of entries in a page range is 8192
    # number of entries in a base page is 512
    page_range_index = math.floor(rid / ENTRIES_PER_PAGE_RANGE)
    index = rid % ENTRIES_PER_PAGE_RANGE
    base_page_index = math.floor(index / ENTRIES_PER_PAGE)
    physical_page_index = index % ENTRIES_PER_PAGE
    return { 'page_range': page_range_index, 'base_page': base_page_index, 'page_index': physical_page_index }

print(__rid_to_page_location(2050))