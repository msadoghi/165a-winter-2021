from lstore.table import *
from random import randint

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
testInsert()