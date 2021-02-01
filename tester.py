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
    pr_index = 3 # len(self.book)
    location_in_pr_index = math.floor((rid - (pr_index-1)*(ENTRIES_PER_PAGE_RANGE))/ENTRIES_PER_PAGE)
    bp_index = math.floor(location_in_pr_index/ENTRIES_PER_PAGE)
    return { 'page_range': pr_index, 'base_page': location_in_pr_index, 'page_index': bp_index }

print(__rid_to_page_location(1024))