import sys
sys.path.append('../')

from lstore.query import *
from lstore.table import *
from lstore.db import *

database = Database()
database.create_table("Students", 6, 0)
table = database.get_table("Students")
query = Query(table)

def test(test_name, expression, expected_result):
    print('\n\n++++TEST RESULT++++')
    if expression != expected_result :
        print(f'\n!!!!\n{test_name}:FAILED\n\texpected:{expected_result}\n\treceived:{expression}\n!!!!\n')
    else:
        print(f'{test_name} SUCCESSFUL')
    print('++++END  RESULT++++\n\n')

# insert a row to test select with
query.insert(*[1,2,3,4,5,6])
# attempt to insert a record with a duplicate key
# this appears to fail quietly, because the first test passes
query.insert(*[1,12,13,14,15,16])
# find the record we inserted with a key of 1
ret_record = query.select(1,0,[1, 1, 1, 1, 1, 1])
test('find record we inserted with a key of 1',
    ret_record[0].user_data,
    [1, 2, 3, 4, 5, 6])
ret_record = query.select(1,0,[0, 1, 0, 1, 0, 1])
test('find specific columns of record with key 1',
    ret_record[0].user_data,
    [None, 2, None, 4, None, 6])
test('try to find record we did not insert',
    query.select(99999,0,[1,1,1,1,1,1]),
    False)

# attempt to insert a record with a negative number
query.insert(*[2,-22,-23,-24,-25,-26])
ret_record = query.select(2,0,[1, 1, 1, 1, 1, 1])
test('stops us from inserting negative numbers',ret_record,False)

# attempt to insert a ridiculously massive number
query.insert(*[3,696969696969696969696969420,32,33,34,35])
ret_record = query.select(3,0,[1, 1, 1, 1, 1, 1])
test('stops us from inserting massive numbers',ret_record,False)

# update a record, see if the update has flushed
query.update(1,*[None, 69, None, None, None, None])
ret_record = query.select(1,0,[1, 1, 1, 1, 1, 1])
test('try to find record 1 after its been updated',
    ret_record[0].user_data,
    [1, 69, 3, 4, 5, 6])

query.update(1,*[None, None, 69, None, None, None])
ret_record = query.select(1,0,[1, 1, 1, 1, 1, 1])
test('try to find record 1 after its been updated a second time',
    ret_record[0].user_data,
    [1, 69, 69, 4, 5, 6])

query.update(1,*[None, None, None, 69, None, None])
ret_record = query.select(1,0,[1, 1, 1, 1, 1, 1])
test('try to find record 1 after its been updated a third time',
    ret_record[0].user_data,
    [1, 69, 69, 69, 5, 6])

query.insert(*[2,2,3,4,5,6])
query.insert(*[3,2,3,4,5,6])
query.insert(*[4,2,3,4,5,6])

test('test sum',query.sum(1,4,0),10)

'''
THESE TESTS FAIL
'''

query.insert(*[5,2,3,4,5,6])
# this updates the key of a record
query.update(5,*[69, None, None, None, None, None])
ret_record = query.select(5,0,[1, 1, 1, 1, 1, 1])
test('try to find record 5 after its key has been updated',
    ret_record[0].user_data,
    [69, 2, 3, 4, 5, 6])

ret_record = query.select(69,0,[1, 1, 1, 1, 1, 1])
test('try to find record 69 (previously record 5) after its been updated a second time',
    ret_record[0].user_data,
    [69, 2, 3, 4, 5, 6])



