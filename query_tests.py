from lstore.query import *
from lstore.table import *
from lstore.db import *

def test_select():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    ret_record = query.select(798329, 0, [0, 1, 0, 1, 0, 1])
    print(ret_record)

#test_select()

def test_insert():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    did_insert = query.insert(999, 1, 2, 3, 4, 5)

# test_insert()

def test_insert_and_see_if_exists():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    did_insert2 = query.insert(53245, 2,3,4,5,6)
    print("did_insert:", did_insert)
    print("did_insert2:", did_insert2)
    ret_value = query.table.record_does_exist(key)
    ret_value2 = query.table.record_does_exist(53245)
    print("ret_value:", ret_value)
    print("ret_value2:", ret_value2)

#test_insert_and_see_if_exists()

def test_insert_and_delete_and_see_if_exists():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    key2 = 53245
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    did_insert2 = query.insert(key2, 2,3,4,5,6)
    print("did_insert:", did_insert)
    did_delete = query.delete(key)
    did_delete2 = query.delete(key2)
    print("did_delete", did_delete)
    print("did_delete2", did_delete2)
    ret_value = query.table.record_does_exist(key)
    ret_value2 = query.table.record_does_exist(key2)
    print("ret_value:", ret_value)
    print("ret_value2:", ret_value2)

#test_insert_and_delete_and_see_if_exists()

def test_insert_and_read():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    validRID = table.record_does_exist(key)
    did_read = table.read_record(validRID)
    print("did_read:", did_read.all_columns)

test_insert_and_read()
